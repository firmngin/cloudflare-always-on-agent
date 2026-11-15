/**
 * Firmngin data-agent runtime for Cloudflare Workers.
 *
 * The Worker is owned and deployed by the Firmngin user (BYOK): ONE deployment
 * executes EVERY worker-mode agent of the workspace. It heartbeats, long-polls
 * the merged inbox, claims turns, runs them against any OpenAI-compatible
 * model endpoint with the user's own provider key, streams tool calls and
 * deltas back to Firmngin, and reports the final result.
 *
 * Turn engine: the default engine runs pi's agent core (vendored under
 * src/vendor/, MIT — see src/vendor/pi/NOTICE): the agent loop with tool
 * calling, MCP tools mapped to pi AgentTool[], and skills (progressive
 * disclosure via a load_skill tool). The adapter layer lives in src/pi/.
 * Set AGENT_ENGINE="builtin" to fall back to the previous hand-rolled loop
 * (kept intact below as executeTurnBuiltin).
 *
 * Inbox pickup uses bounded long-polls, including for an idle workspace with
 * enabled agents. Redis wake signals release the backend waiter as soon as a
 * turn is queued. The drain keeps a waiter for up to DRAIN_BUDGET_MS; immediate
 * empty responses (no agents or admission cap) end it until the next cron tick.
 * The per-isolate overlap guard is best-effort; backend CAS claims remain the
 * authority against duplicate execution across isolates.
 *
 * Firmngin never sees the model provider key and never executes the agent.
 * Agents are created in the Firmngin dashboard — the Worker picks them up
 * automatically; nothing here is per-agent and no redeploy is needed.
 *
 * Durable Objects (plan v1.2 §2.1, §6.1, §9.3): one named SQLite object per
 * agent (`w:{workspace_id}:a:{agent_id}`, see src/durable/agent.ts) owns that
 * agent's recurring schedules through the Cloudflare Agents Lifecycle +
 * Scheduler. The object only turns a fired schedule into an idempotent enqueue
 * call to Firmngin; chat long-poll and model inference stay here on the Worker.
 * `AGENT_DO_SYNC=true` enables the sync, and when it is on the once-a-minute
 * cron hour-bucket scheduler is skipped so one logical task never has two
 * schedulers.
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

import {
  ApiError,
  applyModelRuntimeEnv,
  callModel,
  resolveModelProtocol,
  sseDataLines,
  type ChatMessage,
  type Role,
} from './pi/stream-fn'
import { safeJson, summarize, type McpTool } from './pi/tools'
import { executeTurnPi, type PiEngineDeps } from './pi/engine'
import { inboxPollBudget, isPlaceholderToken, resolveBackendUrl, trimSlash, withRequestTimeout } from './runtime/endpoints'
import { logRuntimeTrace, runtimeTrace, traceRegistration, type RuntimeTrace } from './runtime/diagnostics'
import { durableObjectName, type DesiredSchedule, type WorkspaceAgent as WorkspaceAgentType } from './durable/agent'

// Durable Object class: one named SQLite object per agent (plan v1.2 §2.1).
export { WorkspaceAgent } from './durable/agent'

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
  /**
   * Turn engine: "pi" (default) runs the vendored pi agent core; "builtin"
   * runs the previous hand-rolled loop. Any other value falls back to "pi".
   */
  AGENT_ENGINE?: string
  /**
   * Per-agent Durable Object runtime (Lifecycle + Scheduler). Off by default so
   * a deployment whose schedules still use the cron hour-bucket keeps working;
   * the dashboard/template turns it on once `agent_schedules` sync is live.
   */
  AGENT_DO_SYNC?: string
  /** Named SQLite Durable Object per agent (`WorkspaceAgent`). */
  WORKSPACE_AGENT?: DurableObjectNamespace<WorkspaceAgentType>
}

const WORKER_VERSION = 'firmngin-data-agent-worker/2.2.5'

// Firmngin enforces a per-run deadline of ~5 minutes; finish reporting before
// it so the run's terminal state always comes from the Worker itself.
const TURN_TIMEOUT_MS = 4 * 60_000
// Enabled workspaces hold the request until a turn arrives or its bounded
// wait elapses. No-agent or over-cap responses may return immediately.
const INBOX_BATCH = 5
const MAX_TURNS_PER_INVOCATION = 10
// One invocation keeps polling until this budget is spent instead of quitting
// after a single empty poll. The backend only holds a request while a waiter is
// registered, so a Worker that exits after one 25s poll leaves a ~35s window
// with nobody listening: a message sent then waits for the next cron tick, which
// is what made chat feel slow. Staying inside the invocation covers that window
// and brings pickup down to seconds. The cron wall-clock limit is 15 minutes,
// and CPU spent waiting on the long-poll is effectively zero, so this is cheap.
const DRAIN_BUDGET_MS = 50_000
// An immediate empty response means no waiter was held: let cron retry.
const HELD_POLL_THRESHOLD_MS = 1_000
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
let maxStepsEnv = ''
// "pi" (default) | "builtin": which turn engine runs claimed turns.
let agentEngine: 'pi' | 'builtin' = 'pi'
// Per-agent Durable Object sync (schedules). Off unless AGENT_DO_SYNC=true.
let doSyncEnabled = false
// The current invocation's env binding: the Durable Object namespace is only
// reachable through it, so it is captured by applyEnv().
let runtimeEnv: Env | null = null

// Overlap guards shared by every invocation on this isolate: `draining`
// serializes inbox drains so overlapping cron/manual triggers skip instead of
// double-polling, and `activeTurns` counts turns in flight for the spend cap.
let draining = false
let activeTurns = 0

interface WorkerAgent {
  id: string
  name: string
  model?: string
  provider?: string
  model_base_url?: string
  enabled?: boolean
  config_revision?: number
  /** Workspace the agent belongs to; used for the canonical DO name. */
  workspace_id?: string
}

interface InboxItem {
  agent_id: string
  message: { id: number, role: string, content: string }
  run_id: string
  conversation_id: string
}

export interface Claim {
  message: { id: number, content: string, created_at?: string }
  run_id: string
  conversation_id: string
  history: Array<{ role: string, content: string }>
  // Snapshot accepted with the run; the Worker must not read the mutable
  // agent config for these values. platform_prompt is the platform-level
  // system prompt (capabilities, tool policy) owned by Firmngin; the owner's
  // task_prompt is assembled below it.
  task_prompt: string
  platform_prompt?: string
  model: string
  // Optional so older backends keep working; the Worker falls back to its
  // MODEL_BASE_URL env when they are absent.
  provider?: string
  model_base_url?: string
  // "openai" | "anthropic": which wire format to speak for this agent.
  model_protocol?: string
  config_revision: number
  // Optional pi-style skills (agentskills.io pattern): a skills index is added
  // to the system prompt and a builtin load_skill tool serves the full content
  // on demand. Absent (older backends) -> the feature is inert.
  skills?: Array<{ name?: string, description?: string, content?: string, markdown?: string }>
}

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
export class EventStream {
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
// MCP tools (Streamable HTTP)
// ---------------------------------------------------------------------------

export class McpClient {
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
  const startedAt = Date.now()
  let handled = 0
  try {
    // Sustained drain: poll until the invocation budget is spent. Every extra
    // poll keeps a long-poll waiter registered with the backend, and a queued
    // message wakes it immediately — the pickup latency the dashboard shows as
    // "waiting for the Worker".
    while (handled < MAX_TURNS_PER_INVOCATION && Date.now() - startedAt < DRAIN_BUDGET_MS) {
      const budget = inboxPollBudget(DRAIN_BUDGET_MS - (Date.now() - startedAt))
      if (!budget) return
      const pollStartedAt = Date.now()
      let items: InboxItem[]
      try {
        // Keep cancellation active through response.json(), not just headers:
        // a timeout must release the network request and the BE wake waiter.
        const { response, payload } = await withRequestTimeout(async (signal) => {
          const response = await agentFetch(
            `/inbox?wait=${budget.waitSeconds}s&limit=${INBOX_BATCH}`,
            { method: 'GET', signal },
          )
          const payload: any = await response.json()
          return { response, payload }
        }, budget.timeoutMs, 'inbox poll timed out')
        if (!response.ok) {
          console.error(
            'inbox poll returned non-OK:',
            response.status,
            JSON.stringify(payload ?? null).slice(0, 300),
          )
          return
        }
        items = Array.isArray(payload?.data) ? payload.data : []
      } catch (error) {
        console.error('inbox poll failed:', error instanceof Error ? error.message : error)
        return
      }
      if (!items.length) {
        // Enabled idle agents now long-poll too. An immediate empty response
        // means no agents, wait disabled, or waiter capacity exhausted; retrying
        // it here would recreate the HTTP/DB idle spin.
        if (Date.now() - pollStartedAt < HELD_POLL_THRESHOLD_MS) return
        continue
      }
      for (const item of items) {
        if (activeTurns >= MAX_CONCURRENT_TURNS) {
          console.log('turn concurrency cap reached; leaving rest for the next invocation')
          return
        }
        await executeInboxItem(item)
        handled++
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
    // Queue wait is the part of "chat is slow" that the Worker controls (the
    // rest is model latency). Logging it makes a pickup regression visible in
    // `wrangler tail` instead of only in the dashboard feeling.
    const queuedAt = Date.parse(String(claim.message?.created_at || ''))
    if (Number.isFinite(queuedAt)) {
      console.log(`run ${claim.run_id} claimed ${Date.now() - queuedAt}ms after it was queued`)
    }
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
      agentEngine === 'builtin'
        ? executeTurnBuiltin(agentId, claim, events)
        : executeTurnPi(agentId, claim, events, piEngineDeps()),
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

// Engine wiring: MAX_STEPS is resolved here with the same expression the
// builtin engine uses, so both engines honor the env var identically.
function piEngineDeps(): PiEngineDeps {
  return {
    connectMcp,
    maxSteps: Math.max(1, Number(maxStepsEnv || 8)),
  }
}

async function executeTurnBuiltin(agentId: string, claim: Claim, events: EventStream): Promise<string> {
  const model = claim.model
  if (!model) throw new ApiError(0, 'Agent config has no model', 'CONFIG_INVALID')
  const protocol = resolveModelProtocol(claim)
  const mcp = await connectMcp(agentId)
  const tools = mcp?.tools || []

  // Two-level system prompt: platform capabilities first, owner instruction
  // last. Without platform_prompt (older backends) this is unchanged.
  const platformText = String(claim.platform_prompt || '').trim()
  const taskText = String(claim.task_prompt || '').trim()
  const systemContent = platformText && taskText
    ? `${platformText}\n\n# Owner instructions\n\n${taskText}`
    : (taskText || platformText)

  const messages: ChatMessage[] = [
    { role: 'system', content: systemContent },
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

async function heartbeat(trace: RuntimeTrace): Promise<void> {
  await traceRegistration(trace, apiUrl, workerToken, 'heartbeat', { version: WORKER_VERSION })
}

// Hour-bucket idempotency key per agent: a retried cron within the same hour
// returns the existing queued turn instead of duplicating it. Legacy fallback
// only: an agent whose schedules are owned by its Durable Object must NOT also
// be enqueued from cron (two schedulers for one logical task).
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

/** Agent roster for this workspace Worker token; never throws. */
async function listWorkerAgents(): Promise<WorkerAgent[]> {
  // Preferred: the scoped deployment config. It carries the workspace id the
  // canonical Durable Object name needs and returns only this token's workspace.
  try {
    const response = await agentFetch('/deployment/config', { method: 'GET' })
    if (response.ok) {
      const payload: any = await response.json()
      const data = payload?.data || {}
      const workspaceId = String(data?.workspace_id || '')
      const agents = Array.isArray(data?.agents) ? data.agents : []
      return agents.map((agent: any) => ({ ...agent, workspace_id: workspaceId }))
    }
    if (response.status !== 404) {
      console.error('deployment config failed:', response.status)
    }
  } catch (error) {
    console.error('deployment config failed:', error instanceof Error ? error.message : error)
  }
  // Fallback for a backend that predates the config endpoint.
  try {
    const response = await agentFetch('/agents', { method: 'GET' })
    const payload: any = await response.json()
    return Array.isArray(payload?.data) ? payload.data : []
  } catch (error) {
    console.error('listing agents failed:', error instanceof Error ? error.message : error)
    return []
  }
}

/**
 * Outbound pairing: promotes the workspace deployment pending→connected and
 * refreshes the Worker credential row. Idempotent and replay-safe, so calling it
 * every invocation is cheap; a nonce is sent for correlation only, never as
 * authentication.
 */
async function handshake(trace: RuntimeTrace): Promise<void> {
  await traceRegistration(trace, apiUrl, workerToken, 'handshake', {
    runtime_version: WORKER_VERSION, nonce: crypto.randomUUID(),
  })
}

/**
 * Desired schedules for one agent, as owned by the backend (`agent_schedules`).
 * An absent endpoint (older backend) is a no-op, not an error: DO sync turns on
 * through AGENT_DO_SYNC only after this contract ships.
 */
async function fetchDesiredSchedules(agentId: string): Promise<DesiredSchedule[] | null> {
  try {
    const response = await agentFetch(`/agents/${agentId}/schedules`, { method: 'GET' })
    if (!response.ok) {
      if (response.status !== 404) {
        console.error('schedule sync fetch failed:', response.status)
      }
      return null
    }
    const payload: any = await response.json()
    return Array.isArray(payload?.data) ? payload.data : []
  } catch (error) {
    console.error('schedule sync fetch failed:', error instanceof Error ? error.message : error)
    return null
  }
}

/**
 * Pushes each agent's desired schedules into its named Durable Object
 * (`w:{workspace}:a:{agent}`). The object is created lazily here — never at
 * workspace/agent creation — and the sync is idempotent, so a repeated cron
 * tick changes nothing.
 *
 * Returns the agent ids whose scheduling is now owned by a Durable Object (at
 * least one `agent_schedules` row, enabled or not). Those agents must NOT also
 * be enqueued by the legacy hour-bucket cron; agents without any schedule keep
 * the legacy default so a flag flip does not silently stop their runs.
 */
async function syncAgentDurableObjects(agents: WorkerAgent[]): Promise<Set<string>> {
  const owned = new Set<string>()
  const namespace = runtimeEnv?.WORKSPACE_AGENT
  if (!namespace) {
    console.error('AGENT_DO_SYNC is enabled but the WORKSPACE_AGENT binding is missing; leaving schedules on the cron fallback')
    return owned
  }
  for (const agent of agents) {
    const workspaceId = String(agent.workspace_id || '').trim()
    if (!workspaceId) {
      // Older backends do not return the workspace id; the canonical object
      // name cannot be derived without it, so skip instead of inventing one.
      continue
    }
    try {
      const desired = await fetchDesiredSchedules(agent.id)
      if (!desired) continue
      const stub = namespace.getByName(durableObjectName(workspaceId, agent.id))
      const result = await stub.syncSchedules(desired, Number(agent.config_revision || 0))
      if (desired.length) owned.add(agent.id)
      // Report provisioning/sync state so the dashboard can show the binding
      // without querying Cloudflare. Best-effort: a failure here never blocks
      // the schedule sync that already succeeded.
      try {
        await agentFetch(`/agents/${agent.id}/runtime-binding`, {
          method: 'POST',
          body: JSON.stringify({
            do_name: durableObjectName(workspaceId, agent.id),
            applied_config_version: result.applied_config_version,
          }),
        })
      } catch (error) {
        console.error(`runtime binding report failed for agent ${agent.id}:`, error instanceof Error ? error.message : error)
      }
      if (result.created || result.cancelled) {
        console.log(
          `durable object synced: agent=${agent.id} created=${result.created} cancelled=${result.cancelled} applied=${result.applied_version}`,
        )
      }
    } catch (error) {
      console.error(`durable object sync failed for agent ${agent.id}:`, error instanceof Error ? error.message : error)
    }
  }
  return owned
}

function applyEnv(env: Env) {
  runtimeEnv = env
  apiUrl = resolveBackendUrl(env)
  workerToken = env.WORKER_TOKEN || ''
  maxStepsEnv = env.MAX_STEPS || ''
  applyModelRuntimeEnv(env)
  agentEngine = String(env.AGENT_ENGINE || '').trim().toLowerCase() === 'builtin' ? 'builtin' : 'pi'
  doSyncEnabled = String(env.AGENT_DO_SYNC || '').trim().toLowerCase() === 'true'
}

function isConfigured(): boolean {
  return Boolean(workerToken) && Boolean(apiUrl) && !isPlaceholderToken(workerToken)
}
export default {
  async scheduled(event: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    applyEnv(env)
    const trace = runtimeTrace(env, WORKER_VERSION, 'scheduled')
    logRuntimeTrace(trace, 'runtime.invocation.start', {
      cron: event.cron, scheduled_at: new Date(event.scheduledTime).toISOString(),
      schedule_lag_ms: Date.now() - event.scheduledTime,
    })
    if (!isConfigured()) {
      logRuntimeTrace(trace, 'runtime.configuration.invalid', {}, true)
      console.error('Worker is not configured: set URL_BACKEND (var) and a real WORKER_TOKEN secret (not the wizard placeholder)')
      return
    }
    ctx.waitUntil((async () => {
      // Start polling before anything else: pickup latency is the part of a
      // chat turn the Worker owns, so it must not queue behind the heartbeat
      // and agent-listing round trips.
      const drain = drainInbox()
      await Promise.all([
        heartbeat(trace),
        handshake(trace),
        (async () => {
          const agents = await listWorkerAgents()
          if (doSyncEnabled) {
            // DO-owned agents get their schedules from the object; agents with
            // no schedule rows keep the legacy hourly default so flipping the
            // flag never silently stops their runs.
            const owned = await syncAgentDurableObjects(agents)
            await queueScheduledRuns(agents.filter((agent) => !owned.has(agent.id)))
            return
          }
          await queueScheduledRuns(agents)
        })(),
      ])
      await drain
      logRuntimeTrace(trace, 'runtime.invocation.complete')
    })().catch((error) => {
      logRuntimeTrace(trace, 'runtime.invocation.failed', { failure: 'runtime_task_failure' }, true)
      throw error
    }))
  },

  // GET  / -> heartbeat probe (handy to verify the deploy works)
  // POST / -> drain the inbox now (manual "run pending turns" trigger)
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    applyEnv(env)
    const trace = runtimeTrace(env, WORKER_VERSION, 'fetch')
    logRuntimeTrace(trace, 'runtime.invocation.start', { method: request.method })
    if (!isConfigured()) {
      logRuntimeTrace(trace, 'runtime.configuration.invalid', {}, true)
      return Response.json({ ok: false, error: 'Worker is not configured: set URL_BACKEND (var) and a real WORKER_TOKEN secret (not the wizard placeholder)' }, { status: 500 })
    }
    if (request.method === 'POST') {
      ctx.waitUntil(drainInbox())
      return Response.json({ ok: true, draining: true })
    }
    ctx.waitUntil(heartbeat(trace))
    return Response.json({
      ok: true,
      version: WORKER_VERSION,
      environment: String(env.ENVIRONMENT || '').toLowerCase() === 'development' ? 'development' : 'production',
      backend: apiUrl,
      token_configured: !isPlaceholderToken(workerToken),
    })
  },
}
