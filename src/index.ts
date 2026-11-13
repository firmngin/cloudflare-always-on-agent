/**
 * Firmngin data-agent runtime for Cloudflare Workers.
 *
 * The Worker is owned and deployed by the Firmngin user (BYOK): it fetches the
 * agent config, long-polls the inbox, claims turns, runs them against any
 * OpenAI-compatible model endpoint with the user's own provider key, streams
 * tool calls and deltas back to Firmngin, and reports the final result.
 * Firmngin never sees the model provider key and never executes the agent.
 *
 * Deploy: the Firmngin dashboard "Deploy to Cloudflare" button (this repo), or
 * `npx wrangler deploy` after filling in wrangler.toml and the two secrets:
 *   AGENT_TOKEN    - the agent's `fng_aoa_...` runtime token
 *   MODEL_API_KEY  - the model provider key (never sent to Firmngin)
 */

export interface Env {
  API_URL: string
  AGENT_ID: string
  AGENT_TOKEN: string
  MODEL_API_KEY: string
  MODEL_BASE_URL?: string
  MODEL?: string
  MAX_STEPS?: string
}

const WORKER_VERSION = 'firmngin-data-agent-worker/1.0.0'

// Firmngin enforces a per-run deadline of ~5 minutes; finish reporting before
// it so the run's terminal state always comes from the Worker itself.
const TURN_TIMEOUT_MS = 4 * 60_000
const INBOX_WAIT_SECONDS = 25
const INBOX_BATCH = 5
const MAX_TURNS_PER_INVOCATION = 10
// The claimed message is streamed before persisting deltas; flush batches so a
// crash mid-turn still leaves readable partial output behind.
const EVENT_FLUSH_SIZE = 12

type Role = 'system' | 'user' | 'assistant' | 'tool'

interface ChatMessage {
  role: Role
  content: string
  tool_calls?: Array<{ id: string, type: 'function', function: { name: string, arguments: string } }>
  tool_call_id?: string
  name?: string
}

interface InboxItem {
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

function runtimeUrl(env: Env, path: string): string {
  return `${trimSlash(env.API_URL)}/api/v1/agent-runtime/agents/${encodeURIComponent(env.AGENT_ID)}${path}`
}

async function agentFetch(env: Env, path: string, init: RequestInit = {}): Promise<Response> {
  const response = await fetch(runtimeUrl(env, path), {
    ...init,
    headers: {
      Authorization: `Bearer ${env.AGENT_TOKEN}`,
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
    private readonly env: Env,
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

  get size(): number {
    return this.pending.length
  }

  async flush(): Promise<void> {
    if (!this.pending.length) return
    const events = this.pending
    this.pending = []
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        const response = await agentFetch(this.env, `/runs/${this.runId}/events`, {
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
// Model provider (OpenAI-compatible, BYOK)
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

async function callModel(
  env: Env,
  model: string,
  messages: ChatMessage[],
  tools: McpTool[],
  onDelta: (text: string) => Promise<void>,
): Promise<ModelTurnResult> {
  const base = trimSlash(env.MODEL_BASE_URL || 'https://openrouter.ai/api/v1')
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
      Authorization: `Bearer ${env.MODEL_API_KEY}`,
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
// MCP tools (Streamable HTTP)
// ---------------------------------------------------------------------------

class McpClient {
  private nextId = 1
  private sessionId = ''
  private tools: McpTool[] = []

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
      client.tools = Array.isArray(listed?.tools) ? listed.tools : []
      return client.tools.length ? client : null
    } catch (error) {
      console.error('MCP connect failed:', error instanceof Error ? error.message : error)
      return null
    }
  }

  get tools(): McpTool[] {
    return this.tools
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

async function connectMcp(env: Env): Promise<McpClient | null> {
  try {
    const response = await agentFetch(env, '/mcp-token', { method: 'POST', body: '{}' })
    if (!response.ok) return null
    const token: any = await response.json()
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

async function drainInbox(env: Env): Promise<void> {
  for (let handled = 0; handled < MAX_TURNS_PER_INVOCATION; handled++) {
    let items: InboxItem[]
    try {
      const response = await agentFetch(
        env,
        `/inbox?wait=${INBOX_WAIT_SECONDS}&limit=${INBOX_BATCH}`,
        { method: 'GET' },
      )
      const payload: any = await response.json()
      items = Array.isArray(payload?.data) ? payload.data : []
    } catch (error) {
      // 401/403 (revoked or disabled agent) must not spin the cron loop
      console.error('inbox poll failed:', error instanceof Error ? error.message : error)
      return
    }
    if (!items.length) return
    for (const item of items) {
      await executeInboxItem(env, item)
    }
  }
}

async function executeInboxItem(env: Env, item: InboxItem): Promise<void> {
  const claimToken = crypto.randomUUID()
  let claim: Claim
  try {
    const response = await agentFetch(env, `/inbox/${item.message.id}/claim`, {
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
  await runTurn(env, claim)
}

async function runTurn(env: Env, claim: Claim): Promise<void> {
  const events = new EventStream(env, claim.run_id, claim.conversation_id)
  const started = Date.now()
  try {
    await events.pushAndFlush('message.start')
    const output = await withTimeout(
      executeTurn(env, claim, events),
      TURN_TIMEOUT_MS,
      'Turn exceeded the time budget',
    )
    await events.pushAndFlush('message.final', output)
    await finishRun(env, claim, {
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
    await finishRun(env, claim, {
      status: 'failed',
      error_code: code,
      error_message: message,
    })
  }
}

async function executeTurn(env: Env, claim: Claim, events: EventStream): Promise<string> {
  const model = env.MODEL || claim.model
  if (!model) throw new ApiError(0, 'Agent config has no model', 'CONFIG_INVALID')
  const mcp = await connectMcp(env)
  const tools = mcp?.tools || []

  const messages: ChatMessage[] = [
    { role: 'system', content: claim.task_prompt },
    ...(claim.history || [])
      .filter((item) => item.role === 'user' || item.role === 'assistant')
      .map((item) => ({ role: item.role as Role, content: item.content || '' })),
    { role: 'user', content: claim.message?.content || '' },
  ]

  const maxSteps = Math.max(1, Number(env.MAX_STEPS || 8))
  let finalContent = ''
  for (let step = 0; step < maxSteps; step++) {
    const result = await callModel(env, model, messages, tools, async (text) => {
      events.push('delta', text)
      await events.flushIfLarge()
    })
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

async function finishRun(env: Env, claim: Claim, report: {
  status: 'succeeded' | 'failed'
  content?: string
  output?: string
  error_code?: string
  error_message?: string
}): Promise<void> {
  try {
    await agentFetch(env, `/runs/${claim.run_id}`, {
      method: 'PATCH',
      body: JSON.stringify(report),
    })
  } catch (error) {
    console.error(`finish report failed for run ${claim.run_id}:`, error instanceof Error ? error.message : error)
  }
}

async function heartbeat(env: Env): Promise<void> {
  try {
    await agentFetch(env, '/heartbeat', {
      method: 'POST',
      body: JSON.stringify({ version: WORKER_VERSION }),
    })
  } catch (error) {
    console.error('heartbeat failed:', error instanceof Error ? error.message : error)
  }
}

function isConfigured(env: Env): boolean {
  if (!env.AGENT_ID || env.AGENT_ID === 'REPLACE_WITH_AGENT_ID') return false
  if (!env.AGENT_TOKEN) return false
  return true
}

export default {
  async scheduled(_event: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    if (!isConfigured(env)) {
      console.error('Worker is not configured: set AGENT_ID (var) and AGENT_TOKEN (secret)')
      return
    }
    ctx.waitUntil((async () => {
      await heartbeat(env)
      // Hour-bucket idempotency key: a retried cron within the same hour
      // returns the existing queued turn instead of duplicating it.
      const idempotencyKey = `${env.AGENT_ID}:${new Date().toISOString().slice(0, 13)}`
      try {
        await agentFetch(env, '/scheduled-runs', {
          method: 'POST',
          body: JSON.stringify({ idempotency_key: idempotencyKey }),
        })
      } catch (error) {
        console.error('scheduled-runs failed:', error instanceof Error ? error.message : error)
        return
      }
      await drainInbox(env)
    })())
  },

  // GET  / -> heartbeat probe (handy to verify the deploy works)
  // POST / -> drain the inbox now (manual "run pending turns" trigger)
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    if (!isConfigured(env)) {
      return Response.json({ ok: false, error: 'Worker is not configured: set AGENT_ID (var) and AGENT_TOKEN (secret)' }, { status: 500 })
    }
    if (request.method === 'POST') {
      ctx.waitUntil(drainInbox(env))
      return Response.json({ ok: true, draining: true })
    }
    ctx.waitUntil(heartbeat(env))
    return Response.json({ ok: true, agent_id: env.AGENT_ID, version: WORKER_VERSION })
  },
}
