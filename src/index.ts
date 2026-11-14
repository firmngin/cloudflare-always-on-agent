/**
 * Firmngin data-agent runtime for Cloudflare Workers.
 *
 * The Worker is owned and deployed by the Firmngin user (BYOK): ONE deployment
 * executes EVERY worker-mode agent of the workspace. It heartbeats, long-polls
 * the merged inbox, claims turns, runs them against any OpenAI-compatible
 * model endpoint with the user's own provider key, streams tool calls and
 * deltas back to Firmngin, and reports the final result.
 *
 * Inbox pickup is adaptive: the backend answers the long-poll instantly while
 * the workspace is idle (the once-a-minute cron tick stays the fallback) and
 * holds an active workspace's request until a turn is queued, so replies
 * usually start within seconds. Overlapping invocations are guarded on the
 * Worker side — an in-flight drain is never started twice and at most two
 * turns run at once — while the backend's CAS claim stays the authority
 * against double execution.
 *
 * Firmngin never sees the model provider key and never executes the agent.
 * Agents are created in the Firmngin dashboard — the Worker picks them up
 * automatically; nothing here is per-agent and no redeploy is needed.
 *
 * Deploy: the Firmngin dashboard "Deploy to Cloudflare" button (this repo), or
 * `npx wrangler deploy` after filling in wrangler.toml and the two secrets:
 *   WORKER_TOKEN   - the workspace Worker token (fng_aow_...)
 *   MODEL_API_KEY  - the model provider key (never sent to Firmngin)
 *
 * URL_BACKEND is a plain var holding the production API base URL. A development
 * deployment overrides it at runtime through ENVIRONMENT="development" plus
 * URL_BACKEND_DEV, so no private tunnel URL is ever committed to this repo.
 */

export interface Env {
  URL_BACKEND: string
  /** "development" | "production". Unset means production. */
  ENVIRONMENT?: string
  /** Dev-only backend override. Ignored unless ENVIRONMENT is development. */
  URL_BACKEND_DEV?: string
  WORKER_TOKEN: string
  MODEL_API_KEY: string
  MODEL_BASE_URL?: string
  /** Output cap for providers that require it (Anthropic Messages). */
  MODEL_MAX_TOKENS?: string
  MAX_STEPS?: string
}

const WORKER_VERSION = 'firmngin-data-agent-worker/2.1.0'

// Firmngin enforces a per-run deadline of ~5 minutes; finish reporting before
// it so the run's terminal state always comes from the Worker itself.
const TURN_TIMEOUT_MS = 4 * 60_000
// Adaptive long-poll: an active workspace holds the request until something is
// enqueued or this wait elapses, an idle one answers instantly and the
// once-a-minute cron tick stays the fallback. An instant empty answer is the
// normal idle response, not an error.
const INBOX_WAIT_SECONDS = 25
const INBOX_BATCH = 5
const MAX_TURNS_PER_INVOCATION = 10
// Bounds cost when cron invocations overlap; the BE CAS claim still prevents
// double execution, this only caps spend.
const MAX_CONCURRENT_TURNS = 2
// The claimed message is streamed before persisting deltas; flush batches so a
// crash mid-turn still leaves readable partial output behind.
const EVENT_FLUSH_SIZE = 12

// Per-invocation configuration captured from the env binding; module-scope so
// every helper reads it without threading the env object through each call.
let apiUrl = ''
let workerToken = ''
let modelApiKey = ''
let modelBase = ''
let maxStepsEnv = ''
let modelMaxTokens = ''

// Overlap guards shared by every invocation on this isolate: `draining`
// serializes inbox drains so overlapping cron/manual triggers skip instead of
// double-polling, and `activeTurns` counts turns in flight for the spend cap.
let draining = false
let activeTurns = 0

type Role = 'system' | 'user' | 'assistant' | 'tool'

interface ChatMessage {
  role: Role
  content: string
  tool_calls?: Array<{ id: string, type: 'function', function: { name: string, arguments: string } }>
  tool_call_id?: string
  name?: string
}

interface WorkerAgent {
  id: string
  name: string
  model: string
  provider: string
  model_base_url: string
  enabled: boolean
  config_revision: number
}

interface InboxItem {
  agent_id: string
  message: { id: number, role: string, content: string }
  run_id: string
  conversation_id: string
}

interface Claim {
  message: { id: number, content: string }
  run_id: string
  conversation_id: string
  history: Array<{ role: string, content: string }>
  task_prompt: string
  model: string
  // Optional so older backends keep working; the Worker falls back to its
  // MODEL_BASE_URL env when they are absent.
  provider?: string
  model_base_url?: string
  // "openai" | "anthropic": which wire format to speak for this agent.
  model_protocol?: string
  config_revision: number
}

interface McpTool {
  name: string
  description?: string
  inputSchema?: Record<string, unknown>
}

class ApiError extends Error {
  constructor(readonly status: number, message: string, readonly errorCode = '') {
    super(message)
  }
}

const trimSlash = (value: string) => String(value || '').replace(/\/+$/, '')

function runtimeUrl(path: string): string {
  return `${trimSlash(apiUrl)}/api/v1/agent-runtime/worker${path}`
}

async function agentFetch(path: string, init: RequestInit = {}): Promise<Response> {
  const response = await fetch(runtimeUrl(path), {
    ...init,
    headers: {
      Authorization: `Bearer ${workerToken}`,
      'Content-Type': 'application/json',
      ...(init.headers || {}),
    },
  })
  if (response.status === 401 || response.status === 403) {
    const body: any = await response.json().catch(() => ({}))
    const data = body?.data || body || {}
    throw new ApiError(
      response.status,
      data.message || data.error || `Agent runtime rejected the request (${response.status})`,
      data.error || data.error_code || '',
    )
  }
  return response
}

async function withTimeout<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(message)), ms)
      }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}

/** Buffers run events and posts them in small batches with strictly growing seq. */
class EventStream {
  private seq = 0
  private pending: Record<string, unknown>[] = []

  constructor(
    private readonly agentId: string,
    private readonly runId: string,
    private readonly conversationId: string,
  ) {}

  push(type: string, content = '') {
    this.pending.push({
      run_id: this.runId,
      conversation_id: this.conversationId,
      seq: ++this.seq,
      type,
      content,
      created_at: new Date().toISOString(),
    })
  }

  async flush(): Promise<void> {
    if (!this.pending.length) return
    const events = this.pending
    this.pending = []
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        const response = await agentFetch(`/agents/${this.agentId}/runs/${this.runId}/events`, {
          method: 'POST',
          body: JSON.stringify({ events }),
        })
        if (response.ok) return
      } catch {
        // retried below; a lost delta batch is acceptable, the final report is not
      }
    }
    console.error(`event batch lost (${events.length} events) for run ${this.runId}`)
  }

  async pushAndFlush(type: string, content = '') {
    this.push(type, content)
    await this.flush()
  }

  async flushIfLarge(): Promise<void> {
    if (this.pending.length >= EVENT_FLUSH_SIZE) await this.flush()
  }
}

// ---------------------------------------------------------------------------
// Model provider (OpenAI chat completions + Anthropic Messages, BYOK)
// ---------------------------------------------------------------------------

async function* sseDataLines(response: Response): AsyncGenerator<string> {
  const reader = response.body?.getReader()
  if (!reader) return
  const decoder = new TextDecoder()
  let buffer = ''
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true })
    let newline: number
    while ((newline = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, newline).trim()
      buffer = buffer.slice(newline + 1)
      if (line.startsWith('data:')) yield line.slice(5).trim()
    }
  }
}

interface ToolCallAccumulator {
  id: string
  name: string
  arguments: string
}

interface ModelTurnResult {
  content: string
  toolCalls: Array<{ id: string, name: string, arguments: string }>
}

type ModelProtocol = 'openai' | 'anthropic'

const ANTHROPIC_PROTOCOL_PROVIDERS = new Set(['anthropic', 'claude-compatible', 'claude'])

// The claim carries the protocol from the Firmngin registry; when it is
// missing (older backend), the provider id and finally the base URL host
// decide, so a user pointing MODEL_BASE_URL at api.anthropic.com still gets
// the right wire format.
function resolveModelProtocol(claim: Claim): ModelProtocol {
  const fromClaim = String(claim.model_protocol || '').trim().toLowerCase()
  if (fromClaim === 'anthropic' || fromClaim === 'openai') return fromClaim
  const provider = String(claim.provider || '').trim().toLowerCase()
  if (ANTHROPIC_PROTOCOL_PROVIDERS.has(provider)) return 'anthropic'
  const override = trimSlash(String(claim.model_base_url || modelBase || ''))
  if (override.includes('anthropic')) return 'anthropic'
  return 'openai'
}

function anthropicMaxTokens(): number {
  const parsed = Number(modelMaxTokens)
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 8192
}

async function callModel(
  model: string,
  messages: ChatMessage[],
  tools: McpTool[],
  onDelta: (text: string) => Promise<void>,
  // Per-agent base URL from the claim; the env var stays the fallback so
  // older backends and custom deployments keep working.
  baseUrlOverride?: string,
  protocol: ModelProtocol = 'openai',
): Promise<ModelTurnResult> {
  if (protocol === 'anthropic') {
    return callAnthropicModel(model, messages, tools, onDelta, baseUrlOverride)
  }
  return callOpenAIModel(model, messages, tools, onDelta, baseUrlOverride)
}

async function callOpenAIModel(
  model: string,
  messages: ChatMessage[],
  tools: McpTool[],
  onDelta: (text: string) => Promise<void>,
  baseUrlOverride?: string,
): Promise<ModelTurnResult> {
  const base = trimSlash(baseUrlOverride || modelBase || 'https://openrouter.ai/api/v1')
  const body: Record<string, unknown> = { model, messages, stream: true }
  if (tools.length) {
    body.tools = tools.map((tool) => ({
      type: 'function',
      function: { name: tool.name, description: tool.description || '', parameters: tool.inputSchema || { type: 'object' } },
    }))
  }

  const response = await fetch(`${base}/chat/completions`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${modelApiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  })
  if (!response.ok) {
    const detail: any = await response.json().catch(() => null)
    const message = detail?.error?.message || detail?.message || `Model provider returned ${response.status}`
    if (response.status === 401 || response.status === 403) {
      throw new ApiError(response.status, message, 'MODEL_AUTH_FAILED')
    }
    throw new ApiError(response.status, message, 'MODEL_REQUEST_FAILED')
  }

  let content = ''
  const calls = new Map<number, ToolCallAccumulator>()
  for await (const data of sseDataLines(response)) {
    if (!data || data === '[DONE]') continue
    let chunk: any
    try {
      chunk = JSON.parse(data)
    } catch {
      continue
    }
    const delta = chunk?.choices?.[0]?.delta
    if (!delta) continue
    if (typeof delta.content === 'string' && delta.content.length) {
      content += delta.content
      await onDelta(delta.content)
    }
    for (const call of delta.tool_calls || []) {
      const index = Number(call.index ?? 0)
      const current = calls.get(index) || { id: '', name: '', arguments: '' }
      if (call.id) current.id = call.id
      if (call.function?.name) current.name += call.function.name
      if (call.function?.arguments) current.arguments += call.function.arguments
      calls.set(index, current)
    }
  }
  return {
    content,
    toolCalls: [...calls.values()].filter((call) => call.name),
  }
}

// ---------------------------------------------------------------------------
// Anthropic Messages adapter
// ---------------------------------------------------------------------------

// Anthropic's Messages API is a different wire protocol from OpenAI chat
// completions: x-api-key auth, a required max_tokens, the system prompt as a
// top-level parameter, tool calls as content blocks, and its own SSE events.
// The internal ChatMessage shape stays OpenAI-ish; these helpers translate in
// both directions so executeTurn needs no protocol awareness.

function anthropicEndpoint(baseUrlOverride?: string): string {
  const base = trimSlash(baseUrlOverride || modelBase || 'https://api.anthropic.com')
  return `${base}/v1/messages`
}

interface AnthropicContentBlock {
  type: string
  text?: string
  id?: string
  name?: string
  input?: Record<string, unknown>
  tool_use_id?: string
  content?: string | Array<{ type: string, text?: string }>
}

function toAnthropicMessages(messages: ChatMessage[]): { system: string, messages: Array<{ role: string, content: string | AnthropicContentBlock[] }> } {
  const systemParts: string[] = []
  const out: Array<{ role: string, content: string | AnthropicContentBlock[] }> = []

  for (const message of messages) {
    if (message.role === 'system') {
      if (message.content) systemParts.push(message.content)
      continue
    }
    if (message.role === 'tool') {
      // A tool result must arrive as a user turn holding tool_result blocks.
      // Consecutive tool results merge into one user turn, which is exactly
      // how the OpenAI loop emits them (one per executed call).
      const block: AnthropicContentBlock = {
        type: 'tool_result',
        tool_use_id: message.tool_call_id || '',
        content: message.content || '',
      }
      const previous = out[out.length - 1]
      if (previous && previous.role === 'user' && Array.isArray(previous.content)) {
        previous.content.push(block)
      } else {
        out.push({ role: 'user', content: [block] })
      }
      continue
    }
    if (message.role === 'assistant' && message.tool_calls?.length) {
      const blocks: AnthropicContentBlock[] = []
      if (message.content) blocks.push({ type: 'text', text: message.content })
      for (const call of message.tool_calls) {
        blocks.push({
          type: 'tool_use',
          id: call.id,
          name: call.function.name,
          input: safeJson(call.function.arguments),
        })
      }
      out.push({ role: 'assistant', content: blocks })
      continue
    }
    const role = message.role === 'assistant' ? 'assistant' : 'user'
    if (message.content) out.push({ role, content: message.content })
  }
  return { system: systemParts.join('\n\n'), messages: out }
}

async function callAnthropicModel(
  model: string,
  messages: ChatMessage[],
  tools: McpTool[],
  onDelta: (text: string) => Promise<void>,
  baseUrlOverride?: string,
): Promise<ModelTurnResult> {
  const { system, messages: anthropicMessages } = toAnthropicMessages(messages)
  const body: Record<string, unknown> = {
    model,
    max_tokens: anthropicMaxTokens(),
    stream: true,
    messages: anthropicMessages,
  }
  if (system) body.system = system
  if (tools.length) {
    body.tools = tools.map((tool) => ({
      name: tool.name,
      description: tool.description || '',
      input_schema: tool.inputSchema || { type: 'object' },
    }))
  }

  const response = await fetch(anthropicEndpoint(baseUrlOverride), {
    method: 'POST',
    headers: {
      'x-api-key': modelApiKey,
      'anthropic-version': '2023-06-01',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  })
  if (!response.ok) {
    const detail: any = await response.json().catch(() => null)
    const message = detail?.error?.message || detail?.message || `Model provider returned ${response.status}`
    if (response.status === 401 || response.status === 403) {
      throw new ApiError(response.status, message, 'MODEL_AUTH_FAILED')
    }
    throw new ApiError(response.status, message, 'MODEL_REQUEST_FAILED')
  }

  let content = ''
  // tool_use blocks stream their arguments as input_json_delta fragments, so
  // each accumulator holds the growing JSON string until its block stops.
  const calls = new Map<number, ToolCallAccumulator>()
  let activeToolIndex = -1
  for await (const data of sseDataLines(response)) {
    if (!data) continue
    let chunk: any
    try {
      chunk = JSON.parse(data)
    } catch {
      continue
    }
    if (chunk?.type === 'content_block_start') {
      const block = chunk?.content_block
      if (block?.type === 'tool_use') {
        activeToolIndex = Number(chunk.index ?? 0)
        calls.set(activeToolIndex, {
          id: block.id || `tool_${activeToolIndex}`,
          name: block.name || '',
          arguments: '',
        })
      }
      continue
    }
    if (chunk?.type === 'content_block_delta') {
      const delta = chunk?.delta
      if (delta?.type === 'text_delta' && delta.text) {
        content += delta.text
        await onDelta(delta.text)
      } else if (delta?.type === 'input_json_delta' && delta.partial_json) {
        const current = calls.get(activeToolIndex)
        if (current) current.arguments += delta.partial_json
      }
      continue
    }
    if (chunk?.type === 'content_block_stop') {
      activeToolIndex = -1
      continue
    }
    if (chunk?.type === 'message_stop') break
  }
  return {
    content,
    toolCalls: [...calls.values()].filter((call) => call.name),
  }
}

// ---------------------------------------------------------------------------
// MCP tools (Streamable HTTP)
// ---------------------------------------------------------------------------

class McpClient {
  private nextId = 1
  private sessionId = ''
  private availableTools: McpTool[] = []

  private constructor(private readonly url: string, private readonly token: string) {}

  static async connect(url: string, token: string): Promise<McpClient | null> {
    const client = new McpClient(url, token)
    try {
      const initialized = await client.rpc('initialize', {
        protocolVersion: '2025-03-26',
        capabilities: {},
        clientInfo: { name: 'firmngin-data-agent-worker', version: WORKER_VERSION },
      })
      if (!initialized) return null
      await client.notify('notifications/initialized')
      const listed: any = await client.rpc('tools/list', {})
      client.availableTools = Array.isArray(listed?.tools) ? listed.tools : []
      return client.availableTools.length ? client : null
    } catch (error) {
      console.error('MCP connect failed:', error instanceof Error ? error.message : error)
      return null
    }
  }

  get tools(): McpTool[] {
    return this.availableTools
  }

  async call(name: string, args: Record<string, unknown>): Promise<string> {
    const result: any = await this.rpc('tools/call', { name, arguments: args })
    if (result?.isError) {
      const text = Array.isArray(result?.content)
        ? result.content.map((item: any) => item?.text || '').join('\n')
        : ''
      throw new Error(text || `Tool ${name} failed`)
    }
    return Array.isArray(result?.content)
      ? result.content.map((item: any) => (item?.type === 'text' ? item.text : JSON.stringify(item))).join('\n')
      : JSON.stringify(result ?? {})
  }

  private async rpc(method: string, params: Record<string, unknown>): Promise<any> {
    const id = this.nextId++
    const payload = { jsonrpc: '2.0', id, method, params }
    const response = await fetch(this.url, {
      method: 'POST',
      headers: this.headers(),
      body: JSON.stringify(payload),
    })
    if (!response.ok) {
      throw new Error(`MCP ${method} failed (${response.status})`)
    }
    this.sessionId = response.headers.get('mcp-session-id') || this.sessionId
    const contentType = response.headers.get('content-type') || ''
    if (contentType.includes('text/event-stream')) {
      let latest: any = null
      for await (const data of sseDataLines(response)) {
        if (!data) continue
        try {
          const parsed = JSON.parse(data)
          if (parsed?.id === id) return parsed.result ?? parsed
          latest = parsed
        } catch {
          // keep reading the stream
        }
      }
      return latest?.result ?? null
    }
    const parsed: any = await response.json().catch(() => null)
    if (parsed?.error) throw new Error(parsed.error?.message || `MCP ${method} error`)
    return parsed?.result ?? null
  }

  private async notify(method: string): Promise<void> {
    await fetch(this.url, {
      method: 'POST',
      headers: this.headers(),
      body: JSON.stringify({ jsonrpc: '2.0', method }),
    })
  }

  private headers(): Record<string, string> {
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.token}`,
      'Content-Type': 'application/json',
      Accept: 'application/json, text/event-stream',
    }
    if (this.sessionId) headers['Mcp-Session-Id'] = this.sessionId
    return headers
  }
}

async function connectMcp(agentId: string): Promise<McpClient | null> {
  try {
    const response = await agentFetch(`/agents/${agentId}/mcp-token`, { method: 'POST', body: '{}' })
    if (!response.ok) return null
    const payload: any = await response.json()
    const token = payload?.data || payload
    if (!token?.access_token || !token?.mcp_url) return null
    return await McpClient.connect(token.mcp_url, token.access_token)
  } catch (error) {
    console.error('MCP token exchange failed:', error instanceof Error ? error.message : error)
    return null
  }
}

// ---------------------------------------------------------------------------
// Turn execution
// ---------------------------------------------------------------------------

async function drainInbox(): Promise<void> {
  if (draining) {
    console.log('drain already active; skipping overlapping invocation')
    return
  }
  draining = true
  try {
    for (let handled = 0; handled < MAX_TURNS_PER_INVOCATION; handled++) {
      let items: InboxItem[]
      try {
        // The backend may hold the request for the full long-poll wait; the
        // +15s margin covers transport overhead. A timeout is treated like any
        // other poll failure: log it and stop instead of spinning.
        const response = await withTimeout(
          agentFetch(
            `/inbox?wait=${INBOX_WAIT_SECONDS}&limit=${INBOX_BATCH}`,
            { method: 'GET' },
          ),
          (INBOX_WAIT_SECONDS + 15) * 1000,
          'inbox poll timed out',
        )
        const payload: any = await response.json()
        items = Array.isArray(payload?.data) ? payload.data : []
      } catch (error) {
        // 401/403 (revoked token) must not spin the cron loop
        console.error('inbox poll failed:', error instanceof Error ? error.message : error)
        return
      }
      if (!items.length) return
      for (const item of items) {
        if (activeTurns >= MAX_CONCURRENT_TURNS) {
          console.log('turn concurrency cap reached; leaving rest for the next invocation')
          break
        }
        await executeInboxItem(item)
      }
    }
  } finally {
    draining = false
  }
}

async function executeInboxItem(item: InboxItem): Promise<void> {
  // Counted for the concurrency cap from claim through report so overlapping
  // invocations cannot push the isolate past MAX_CONCURRENT_TURNS.
  activeTurns++
  try {
    const claimToken = crypto.randomUUID()
    let claim: Claim
    try {
      const response = await agentFetch(`/agents/${item.agent_id}/inbox/${item.message.id}/claim`, {
        method: 'POST',
        headers: { 'X-Claim-Token': claimToken },
        body: '{}',
      })
      const payload: any = await response.json()
      claim = payload?.data
    } catch (error) {
      console.error(`claim failed for message ${item.message.id}:`, error instanceof Error ? error.message : error)
      return
    }
    if (!claim?.run_id) return
    await runTurn(item.agent_id, claim)
  } finally {
    activeTurns--
  }
}

async function runTurn(agentId: string, claim: Claim): Promise<void> {
  const events = new EventStream(agentId, claim.run_id, claim.conversation_id)
  const started = Date.now()
  try {
    await events.pushAndFlush('message.start')
    const output = await withTimeout(
      executeTurn(agentId, claim, events),
      TURN_TIMEOUT_MS,
      'Turn exceeded the time budget',
    )
    await events.pushAndFlush('message.final', output)
    await finishRun(agentId, claim, {
      status: 'succeeded',
      content: output,
      output,
    })
    console.log(`run ${claim.run_id} succeeded in ${Date.now() - started}ms`)
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unexpected worker failure'
    const code = error instanceof ApiError && error.errorCode ? error.errorCode : 'WORKER_ERROR'
    console.error(`run ${claim.run_id} failed:`, message)
    await events.pushAndFlush('error', message).catch(() => {})
    await finishRun(agentId, claim, {
      status: 'failed',
      error_code: code,
      error_message: message,
    })
  }
}

async function executeTurn(agentId: string, claim: Claim, events: EventStream): Promise<string> {
  const model = claim.model
  if (!model) throw new ApiError(0, 'Agent config has no model', 'CONFIG_INVALID')
  const protocol = resolveModelProtocol(claim)
  const mcp = await connectMcp(agentId)
  const tools = mcp?.tools || []

  const messages: ChatMessage[] = [
    { role: 'system', content: claim.task_prompt },
    ...(claim.history || [])
      .filter((item) => item.role === 'user' || item.role === 'assistant')
      .map((item) => ({ role: item.role as Role, content: item.content || '' })),
    { role: 'user', content: claim.message?.content || '' },
  ]

  const maxSteps = Math.max(1, Number(maxStepsEnv || 8))
  let finalContent = ''
  for (let step = 0; step < maxSteps; step++) {
    const result = await callModel(model, messages, tools, async (text) => {
      events.push('delta', text)
      await events.flushIfLarge()
    }, claim.model_base_url, protocol)
    if (result.content) finalContent = result.content

    if (!result.toolCalls.length) break

    messages.push({
      role: 'assistant',
      content: result.content,
      tool_calls: result.toolCalls.map((call) => ({
        id: call.id || `call_${step}_${call.name}`,
        type: 'function' as const,
        function: { name: call.name, arguments: call.arguments || '{}' },
      })),
    })

    for (const call of result.toolCalls) {
      let toolOutput = ''
      try {
        if (!mcp) throw new Error('Tool execution is unavailable')
        await events.pushAndFlush('tool.call', JSON.stringify({ name: call.name, arguments: safeJson(call.arguments) }))
        toolOutput = await mcp.call(call.name, safeJson(call.arguments))
        await events.pushAndFlush('tool.result', JSON.stringify({ name: call.name, summary: summarize(toolOutput) }))
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Tool failed'
        await events.pushAndFlush('tool.result', JSON.stringify({ name: call.name, error: message }))
        toolOutput = `Tool error: ${message}`
      }
      messages.push({ role: 'tool', tool_call_id: call.id || `call_${step}_${call.name}`, content: toolOutput })
    }
    finalContent = ''
  }

  await events.flush()
  return finalContent
}

function safeJson(value: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(value || '{}')
    return parsed && typeof parsed === 'object' ? parsed : {}
  } catch {
    return {}
  }
}

function summarize(value: string): string {
  const text = String(value || '').replace(/\s+/g, ' ').trim()
  return text.length > 240 ? `${text.slice(0, 240)}…` : text
}

async function finishRun(agentId: string, claim: Claim, report: {
  status: 'succeeded' | 'failed'
  content?: string
  output?: string
  error_code?: string
  error_message?: string
}): Promise<void> {
  try {
    await agentFetch(`/agents/${agentId}/runs/${claim.run_id}`, {
      method: 'PATCH',
      body: JSON.stringify(report),
    })
  } catch (error) {
    console.error(`finish report failed for run ${claim.run_id}:`, error instanceof Error ? error.message : error)
  }
}

async function heartbeat(): Promise<void> {
  try {
    await agentFetch('/heartbeat', {
      method: 'POST',
      body: JSON.stringify({ version: WORKER_VERSION }),
    })
  } catch (error) {
    console.error('heartbeat failed:', error instanceof Error ? error.message : error)
  }
}

// Hour-bucket idempotency key per agent: a retried cron within the same hour
// returns the existing queued turn instead of duplicating it.
async function queueScheduledRuns(agents: WorkerAgent[]): Promise<void> {
  const hourBucket = new Date().toISOString().slice(0, 13)
  for (const agent of agents) {
    try {
      await agentFetch(`/agents/${agent.id}/scheduled-runs`, {
        method: 'POST',
        body: JSON.stringify({ idempotency_key: `${agent.id}:${hourBucket}` }),
      })
    } catch (error) {
      console.error(`scheduled run for agent ${agent.id} failed:`, error instanceof Error ? error.message : error)
    }
  }
}

function resolveBackendUrl(env: Env): string {
  const isDev = String(env.ENVIRONMENT || '').trim().toLowerCase() === 'development'
  const devUrl = String(env.URL_BACKEND_DEV || '').trim()
  // The dev override only applies when the deployment is explicitly marked as
  // development, so a stray URL_BACKEND_DEV on production cannot redirect it.
  if (isDev && devUrl) return trimSlash(devUrl)
  return trimSlash(String(env.URL_BACKEND || '').trim())
}

function applyEnv(env: Env) {
  apiUrl = resolveBackendUrl(env)
  workerToken = env.WORKER_TOKEN || ''
  modelApiKey = env.MODEL_API_KEY || ''
  modelBase = env.MODEL_BASE_URL || ''
  maxStepsEnv = env.MAX_STEPS || ''
  modelMaxTokens = env.MODEL_MAX_TOKENS || ''
}

function isConfigured(): boolean {
  return Boolean(workerToken) && Boolean(apiUrl) && !isPlaceholder(workerToken)
}

// The wizard pre-fills secrets from .dev.vars.example, so a user who clicks
// through without editing deploys the placeholder verbatim. That token passes a
// mere emptiness check and then fails every call with an opaque 401, which reads
// like "the Worker is broken" instead of "the secret was never filled in".
function isPlaceholder(value: string): boolean {
  const token = String(value || '').trim()
  if (!token) return true
  return /^replace_with/i.test(token) || /\breplace_me\b/i.test(token)
}
export default {
  async scheduled(_event: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    applyEnv(env)
    if (!isConfigured()) {
      console.error('Worker is not configured: set URL_BACKEND (var) and a real WORKER_TOKEN secret (not the wizard placeholder)')
      return
    }
    ctx.waitUntil((async () => {
      await heartbeat()
      let agents: WorkerAgent[] = []
      try {
        const response = await agentFetch('/agents', { method: 'GET' })
        const payload: any = await response.json()
        agents = Array.isArray(payload?.data) ? payload.data : []
      } catch (error) {
        console.error('listing agents failed:', error instanceof Error ? error.message : error)
      }
      await queueScheduledRuns(agents)
      await drainInbox()
    })())
  },

  // GET  / -> heartbeat probe (handy to verify the deploy works)
  // POST / -> drain the inbox now (manual "run pending turns" trigger)
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    applyEnv(env)
    if (!isConfigured()) {
      return Response.json({ ok: false, error: 'Worker is not configured: set URL_BACKEND (var) and a real WORKER_TOKEN secret (not the wizard placeholder)' }, { status: 500 })
    }
    if (request.method === 'POST') {
      ctx.waitUntil(drainInbox())
      return Response.json({ ok: true, draining: true })
    }
    ctx.waitUntil(heartbeat())
    return Response.json({
      ok: true,
      version: WORKER_VERSION,
      environment: String(env.ENVIRONMENT || '').toLowerCase() === 'development' ? 'development' : 'production',
      backend: apiUrl,
      token_configured: !isPlaceholder(workerToken),
    })
  },
}
