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
 * Capacity is per execution lane: the backend assigns interactive vs background
 * from the run source, and each lane has its own pool, so a background analysis
 * never holds a chat slot. Both lanes are planned before a turn starts and are
 * dispatched concurrently inside one poll.
 * The per-isolate overlap guard is best-effort; backend CAS claims remain the
 * authority against duplicate execution across isolates.
 *
 * Subrequests are budgeted per invocation (src/runtime/subrequest-budget.ts):
 * `scheduled()` and each `/v1/wake` request create their own budget for their
 * own lifetime, so overlapping invocations on one isolate never share, reset or
 * poison a counter. Inside one invocation UI event batches and retries are
 * coalesced and cut first, essential work (claims, model steps, MCP, polls)
 * keeps a headroom, and every claimed run reserves its own terminal report
 * BEFORE the claim, so one long streamed answer — or three concurrent ones —
 * can no longer outspend the report that ends a run. The budget assumes the
 * Workers Free floor (50) unless SUBREQUEST_BUDGET raises it.
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
 * `npx wrangler deploy` after filling in wrangler.toml and the three secrets:
 *   WORKER_TOKEN   - the workspace Worker token (fng_aow_...)
 *   MODEL_API_KEY  - the model provider key (never sent to Firmngin)
 *   WAKE_SECRET    - the independent backend-to-Worker HMAC key
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
import { normalizeLane } from './runtime/turn'
import {
  classifyRunReport,
  shouldCompleteWake,
  shouldConnectMcp,
  shouldFlushFirstDelta,
  planLaneBatches,
  BACKGROUND_LANE,
  INTERACTIVE_LANE,
  type RunReportOutcome,
  type TerminalRunReport,
} from './runtime/turn'
import { toTerminalReport, type PendingRunReport } from './runtime/pending-report'
import { verifyWakeSignature, WakeAuthError } from './runtime/wake-auth'
import { classifyRuntimeRoute, resolveWakeCallbackUrl, type ResolvedWakeCallback } from './runtime/wake-route'
import { readBoundedRequestBody, RequestBodyTooLargeError } from './runtime/bounded-body'
import {
  createSubrequestBudget,
  fetchWithBudget,
  isSubrequestLimitError,
  shouldFlushEventBatch,
  SubrequestBudgetExhaustedError,
  REPORT_ATTEMPTS_PER_RUN,
  type RunTerminalReservation,
  type SubrequestBudget,
  type SubrequestCharge,
  type SubrequestTier,
} from './runtime/subrequest-budget'
import { scheduleSyncOwnsAgent } from './runtime/schedule-ownership'
import { durableObjectName, type DesiredSchedule, type WorkspaceAgent as WorkspaceAgentType } from './durable/agent'

// Durable Object class: one named SQLite object per agent (plan v1.2 §2.1).
export { WorkspaceAgent } from './durable/agent'
export { shouldConnectMcp, shouldFlushFirstDelta, turnBatchCapacity, planLaneBatches, normalizeLane } from './runtime/turn'

export interface Env {
  URL_BACKEND: string
  /** "development" | "production". Unset means production. */
  ENVIRONMENT?: string
  /** Dev-only backend override. Ignored unless ENVIRONMENT is development. */
  URL_BACKEND_DEV?: string
  WORKER_TOKEN: string
  MODEL_API_KEY: string
  /** Independently provisioned HMAC key for backend -> Worker wakes. */
  WAKE_SECRET?: string
  /** Preferred: this Worker's public HTTPS origin, e.g. https://worker.example.com. */
  WORKER_BASE_URL?: string
  /** Legacy: the full public HTTPS route ending in /v1/wake. */
  WAKE_CALLBACK_URL?: string
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
  /**
   * Subrequests per invocation this deployment may raise. Mirrors the account's
   * `[limits] subrequests` (50 on Workers Free, 10,000 on Paid). Unset means the
   * Free-plan floor, so the runtime never assumes a paid plan.
   */
  SUBREQUEST_BUDGET?: string
}

const WORKER_VERSION = 'firmngin-data-agent-worker/2.2.9'

// Firmngin enforces a per-run deadline of ~5 minutes; finish reporting before
// it so the run's terminal state always comes from the Worker itself.
const TURN_TIMEOUT_MS = 4 * 60_000
// Enabled workspaces hold the request until a turn arrives or its bounded
// wait elapses. No-agent or over-cap responses may return immediately.
const INBOX_BATCH = 5
const MAX_TURNS_PER_INVOCATION = 10
// One invocation keeps polling for nearly the full one-minute cron interval
// instead of quitting after one empty poll. inboxPollBudget() reserves transport
// time inside this budget; the remaining nominal inter-cron gap is ~1 second,
// subject to Cloudflare scheduling and network jitter. The cron wall-clock limit
// is 15 minutes, and CPU spent waiting on the long-poll is effectively zero.
const DRAIN_BUDGET_MS = 59_000
// An immediate empty response means no waiter was held: let cron retry.
const HELD_POLL_THRESHOLD_MS = 1_000
// Bounds cost when cron invocations overlap; the BE CAS claim still prevents
// double execution, this only caps spend.
//
// Capacity is per lane, not per isolate. The backend assigns the lane from the
// run source (interactive chat vs scheduled/manual/analysis), so a background
// analysis can never occupy a chat slot. Before lanes, one streaming analysis
// left a single slot for every chat turn on the isolate.
const MAX_CONCURRENT_TURNS = 2
const MAX_BACKGROUND_TURNS = 1
// Turns that may run at the same time in one invocation; they share its
// subrequest budget, so each one's best-effort share is derived from this and
// every claimed run reserves its own terminal headroom before the claim.
const TURN_SLOTS = MAX_CONCURRENT_TURNS + MAX_BACKGROUND_TURNS
// Buffered events are bounded: past this, the oldest deltas are dropped so a
// long answer cannot grow the isolate's memory while events are deferred.
const EVENT_PENDING_LIMIT = 256

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
// double-polling, and the lane counters hold turns in flight for the spend cap.
let draining = false
const activeTurnsByLane: Record<string, number> = {
  [INTERACTIVE_LANE]: 0,
  [BACKGROUND_LANE]: 0,
}

/** Lane capacity: chat keeps its own pool, background work gets a smaller one. */
function laneTurnCap(lane?: string | null): number {
  return normalizeLane(lane) === BACKGROUND_LANE ? MAX_BACKGROUND_TURNS : MAX_CONCURRENT_TURNS
}

/** Turns in flight for one lane. */
function activeTurnsFor(lane?: string | null): number {
  return activeTurnsByLane[normalizeLane(lane)] || 0
}

function tryReserveTurn(lane?: string | null): boolean {
  const key = normalizeLane(lane)
  const active = activeTurnsByLane[key] || 0
  if (active >= laneTurnCap(key)) return false
  activeTurnsByLane[key] = active + 1
  return true
}

function releaseTurn(lane?: string | null): void {
  const key = normalizeLane(lane)
  activeTurnsByLane[key] = Math.max(0, (activeTurnsByLane[key] || 0) - 1)
}

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
  /** Backend-assigned execution lane (interactive|background). */
  lane?: string
}

interface WakePayload {
  event_id: string
  deployment_id: string
  workspace_id: string
  agent_id: string
  run_id: string
  message_id: number
  /** Backend-assigned execution lane (interactive|background). */
  lane?: string
}

export interface Claim {
  message: { id: number, content: string, created_at?: string }
  run_id: string
  conversation_id: string
  history: Array<{ role: string, content: string }>
  /** Backend-assigned execution lane. The slot was reserved for this lane. */
  lane?: string
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
  mcp_enabled?: boolean
  // Optional pi-style skills (agentskills.io pattern): a skills index is added
  // to the system prompt and a builtin load_skill tool serves the full content
  // on demand. Absent (older backends) -> the feature is inert.
  skills?: Array<{ name?: string, description?: string, content?: string, markdown?: string }>
}

function runtimeUrl(path: string): string {
  return `${trimSlash(apiUrl)}/api/v1/agent-runtime/worker${path}`
}

async function agentFetch(
  path: string,
  init: RequestInit,
  budget: SubrequestBudget,
  charge: SubrequestCharge = 'essential',
): Promise<Response> {
  const response = await fetchWithBudget(budget, runtimeUrl(path), {
    ...init,
    headers: {
      Authorization: `Bearer ${workerToken}`,
      'Content-Type': 'application/json',
      ...(init.headers || {}),
    },
  }, charge)
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

function abortError(signal?: AbortSignal): Error {
  return signal?.reason instanceof Error ? signal.reason : new Error('Request aborted')
}

/** Event flushes: shared best-effort/essential batches, or the run's own terminal batch. */
export type EventFlushTier = SubrequestTier | 'terminal'

/** Buffers run events and posts them in bounded, budget-aware batches with strictly growing seq. */
export class EventStream {
  private seq = 0
  private pending: Record<string, unknown>[] = []
  private pendingBytes = 0
  private firstDeltaFlushed = false
  private lastFlushAt = Date.now()
  // Best-effort subrequests this turn may spend. Essential work comes from the
  // invocation budget, terminal work from this run's own reservation.
  private readonly bestEffortAllowance: number
  private bestEffortSpent = 0
  private bufferNoted = false

  constructor(
    private readonly budget: SubrequestBudget,
    private readonly reservation: RunTerminalReservation,
    private readonly agentId: string,
    private readonly runId: string,
    private readonly conversationId: string,
  ) {
    this.bestEffortAllowance = budget.bestEffortAllowance(TURN_SLOTS)
  }

  push(type: string, content = '') {
    this.pending.push({
      run_id: this.runId,
      conversation_id: this.conversationId,
      seq: ++this.seq,
      type,
      content,
      created_at: new Date().toISOString(),
    })
    this.pendingBytes += content.length + 128
    if (this.pending.length > EVENT_PENDING_LIMIT) this.dropOldestDelta()
  }

  /** Keeps the buffer bounded: the oldest delta (UI text only) goes first. */
  private dropOldestDelta() {
    const index = this.pending.findIndex((event) => event.type === 'delta')
    const [dropped] = this.pending.splice(index === -1 ? 0 : index, 1)
    if (typeof dropped?.content === 'string') this.pendingBytes -= dropped.content.length + 128
  }

  /**
   * One POST per flush, retried once while the tier still has headroom. A failed
   * batch is re-queued instead of dropped: seq stays monotonic and the next
   * flush (or the terminal message.final/error batch) still delivers it. The
   * terminal batch spends this run's own reserved slot, never another run's.
   */
  async flush(tier: EventFlushTier = 'bestEffort', signal?: AbortSignal): Promise<void> {
    if (!this.pending.length) return
    if (!this.canFlush(tier)) {
      if (!this.bufferNoted) {
        this.bufferNoted = true
        console.log({
          event: 'run.stage', run_id: this.runId, stage: 'event_budget_exhausted',
          pending: this.pending.length, reason: tier,
        })
      }
      return
    }
    const events = this.pending
    const bytes = this.pendingBytes
    this.pending = []
    this.pendingBytes = 0
    let delivered = false
    let deferred = false
    for (let attempt = 0; attempt < 2; attempt++) {
      if (attempt > 0 && !this.canFlush(tier)) break
      if (tier === 'bestEffort') this.bestEffortSpent++
      try {
        const response = await agentFetch(
          `/agents/${this.agentId}/runs/${this.runId}/events`,
          { method: 'POST', body: JSON.stringify({ events }), signal },
          this.budget,
          tier === 'terminal' ? () => this.reservation.takeTerminalEvents() : tier,
        )
        if (response.ok) {
          delivered = true
          break
        }
      } catch (error) {
        if (signal?.aborted) {
          this.pending = events.concat(this.pending)
          this.pendingBytes += bytes
          throw abortError(signal)
        }
        if (error instanceof SubrequestBudgetExhaustedError || isSubrequestLimitError(error)) {
          deferred = true
          break
        }
        // transient: retried below while the tier still has headroom
      }
    }
    if (delivered) {
      this.lastFlushAt = Date.now()
      return
    }
    this.pending = events.concat(this.pending)
    this.pendingBytes += bytes
    this.trimPending()
    this.lastFlushAt = Date.now()
    console.log({
      event: 'run.stage', run_id: this.runId, stage: 'event_batch_deferred',
      events: events.length, budget_exhausted: deferred || this.budget.exhausted,
    })
  }

  /** Re-queued batches can pass the push-time cap, so the buffer is re-trimmed. */
  private trimPending() {
    while (this.pending.length > EVENT_PENDING_LIMIT) this.dropOldestDelta()
  }

  private canFlush(tier: EventFlushTier): boolean {
    if (tier === 'terminal') return this.reservation.terminalEventsLeft > 0
    if (tier !== 'bestEffort') return this.budget.allows(tier)
    return this.bestEffortSpent < this.bestEffortAllowance && this.budget.allows('bestEffort')
  }

  async pushAndFlush(type: string, content = '', signal?: AbortSignal, tier: EventFlushTier = 'bestEffort') {
    this.push(type, content)
    await this.flush(tier, signal)
  }

  async flushIfLarge(signal?: AbortSignal): Promise<void> {
    if (!this.pending.length) return
    if (shouldFlushEventBatch({
      events: this.pending.length,
      bytes: this.pendingBytes,
      sinceFlushMs: Date.now() - this.lastFlushAt,
      spent: this.bestEffortSpent,
      allowance: this.bestEffortAllowance,
      firstFlushed: this.firstDeltaFlushed,
    })) await this.flush('bestEffort', signal)
  }

  async pushDelta(content: string, signal?: AbortSignal): Promise<void> {
    this.push('delta', content)
    if (shouldFlushFirstDelta(this.firstDeltaFlushed)) {
      this.firstDeltaFlushed = true
      const startedAt = Date.now()
      await this.flush('bestEffort', signal)
      console.log({ event: 'run.stage', run_id: this.runId, stage: 'first_delta_backend_flush', duration_ms: Date.now() - startedAt })
      return
    }
    await this.flushIfLarge(signal)
  }
}

// ---------------------------------------------------------------------------
// MCP tools (Streamable HTTP)
// ---------------------------------------------------------------------------

export class McpClient {
  private nextId = 1
  private sessionId = ''
  private availableTools: McpTool[] = []

  private constructor(
    private readonly budget: SubrequestBudget,
    private readonly url: string,
    private readonly token: string,
    private readonly signal?: AbortSignal,
  ) {}

  static async connect(budget: SubrequestBudget, url: string, token: string, signal?: AbortSignal): Promise<McpClient | null> {
    const client = new McpClient(budget, url, token, signal)
    try {
      const initialized = await client.rpc('initialize', {
        protocolVersion: '2025-03-26',
        capabilities: {},
        clientInfo: { name: 'firmngin-data-agent-worker', version: WORKER_VERSION },
      }, signal)
      if (!initialized) return null
      await client.notify('notifications/initialized', signal)
      const listed: any = await client.rpc('tools/list', {}, signal)
      client.availableTools = Array.isArray(listed?.tools) ? listed.tools : []
      return client.availableTools.length ? client : null
    } catch (error) {
      if (signal?.aborted) throw abortError(signal)
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

  private async rpc(method: string, params: Record<string, unknown>, signal: AbortSignal | undefined = this.signal): Promise<any> {
    const id = this.nextId++
    const payload = { jsonrpc: '2.0', id, method, params }
    const response = await fetchWithBudget(this.budget, this.url, {
      method: 'POST',
      headers: this.headers(),
      body: JSON.stringify(payload),
      signal,
    }, 'essential')
    if (!response.ok) {
      throw new Error(`MCP ${method} failed (${response.status})`)
    }
    this.sessionId = response.headers.get('mcp-session-id') || this.sessionId
    const contentType = response.headers.get('content-type') || ''
    if (contentType.includes('text/event-stream')) {
      let latest: any = null
      for await (const data of sseDataLines(response, signal)) {
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

  private async notify(method: string, signal: AbortSignal | undefined = this.signal): Promise<void> {
    await fetchWithBudget(this.budget, this.url, {
      method: 'POST',
      headers: this.headers(),
      body: JSON.stringify({ jsonrpc: '2.0', method }),
      signal,
    }, 'essential')
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

async function connectMcp(agentId: string, budget: SubrequestBudget, signal?: AbortSignal): Promise<McpClient | null> {
  try {
    const response = await agentFetch(`/agents/${agentId}/mcp-token`, { method: 'POST', body: '{}', signal }, budget)
    if (!response.ok) return null
    const payload: any = await response.json()
    const token = payload?.data || payload
    if (!token?.access_token || !token?.mcp_url) return null
    return await McpClient.connect(budget, token.mcp_url, token.access_token, signal)
  } catch (error) {
    if (signal?.aborted) throw abortError(signal)
    console.error('MCP token exchange failed:', error instanceof Error ? error.message : error)
    return null
  }
}

// ---------------------------------------------------------------------------
// Turn execution
// ---------------------------------------------------------------------------

async function drainInbox(budget: SubrequestBudget): Promise<void> {
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
      // Every poll is a subrequest. Once this invocation cannot afford one, stop
      // draining instead of failing the poll: the next tick starts with a fresh
      // budget and the queued turn is still there.
      if (!budget.allows('essential')) {
        console.log('subrequest budget spent; ending drain until the next invocation')
        return
      }
      const pollBudget = inboxPollBudget(DRAIN_BUDGET_MS - (Date.now() - startedAt))
      if (!pollBudget) return
      const pollStartedAt = Date.now()
      let items: InboxItem[]
      try {
        // Keep cancellation active through response.json(), not just headers:
        // a timeout must release the network request and the BE wake waiter.
        const { response, payload } = await withRequestTimeout(async (signal) => {
          const response = await agentFetch(
            `/inbox?wait=${pollBudget.waitSeconds}s&limit=${INBOX_BATCH}`,
            { method: 'GET', signal },
            budget,
          )
          const payload: any = await response.json()
          return { response, payload }
        }, pollBudget.timeoutMs, 'inbox poll timed out')
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
      // Group by lane so capacity accounting is per lane: a background batch
      // can never take a chat slot, and a full background lane must not stop
      // chat turns from being claimed in the same poll.
      const byLane = new Map<string, InboxItem[]>()
      for (const item of items) {
        const lane = normalizeLane(item.lane)
        const laneItems = byLane.get(lane)
        if (laneItems) laneItems.push(item)
        else byLane.set(lane, [item])
      }
      const { batches, reserved, skipped } = planLaneBatches(
        byLane,
        activeTurnsFor,
        laneTurnCap,
        handled,
        MAX_TURNS_PER_INVOCATION,
      )
      for (const lane of skipped) {
        console.log(`turn capacity reached for ${lane} lane; leaving the rest for the next invocation`)
      }
      if (!batches.length) {
        // Every lane is already at capacity (or the invocation limit is spent).
        // Re-polling would return the same rows, so leave them for the next tick.
        return
      }
      // Claiming a turn that cannot stream, use a model, and report its terminal
      // state would only burn the budget that the next invocation needs. Leave
      // the rows queued (the backend keeps them claimable) and end the drain.
      if (!budget.allows('bestEffort')) {
        console.log(`subrequest budget spent; leaving ${reserved} queued turn(s) for the next invocation`)
        return
      }
      // Dispatch the lanes together: serializing them here is what let a full
      // background lane delay chat pickup for the whole poll. A turn whose
      // terminal report this invocation cannot reserve is left queued, so the
      // drain ends instead of re-polling rows it cannot finish.
      const laneResults = await Promise.all(batches.map(({ items: laneItems }) =>
        Promise.allSettled(laneItems.map((item) => executeInboxItem(item, budget))),
      ))
      handled += reserved
      if (laneResults.some((lane) => lane.some((result) => result.status === 'fulfilled' && !result.value))) return
    }
  } finally {
    draining = false
  }
}

/** Runs one claimed turn; false means this invocation could not reserve its report. */
async function executeInboxItem(item: InboxItem, budget: SubrequestBudget): Promise<boolean> {
  // Counted for the lane's cap from claim through report so overlapping
  // invocations cannot push the isolate past it.
  const lane = normalizeLane(item.lane)
  if (!tryReserveTurn(lane)) return true
  // The terminal report is reserved BEFORE the claim: a turn this invocation
  // cannot report is left queued instead of claimed and then stalled.
  const reservation = budget.reserveRun(item.run_id)
  if (!reservation) {
    releaseTurn(lane)
    console.log({
      event: 'run.stage', run_id: item.run_id, stage: 'terminal_reserve_unavailable',
      subrequests_remaining: budget.remaining(), subrequests_reserved: budget.reserved,
    })
    return false
  }
  try {
    const claim = await claimTurn(budget, item.agent_id, item.message.id, crypto.randomUUID(), item.run_id)
    if (!claim) return true
    // The backend binds the lane to the run. A mismatch means a stale wake or
    // inbox snapshot; the reserved slot is kept (the turn is real) but the
    // mismatch is logged so the lane contract cannot drift silently.
    if (claim.lane && normalizeLane(claim.lane) !== lane) {
      console.warn(
        `lane mismatch for run ${claim.run_id}: reserved=${lane} claimed=${normalizeLane(claim.lane)}`,
      )
    }
    // Queue wait is the part of "chat is slow" that the Worker controls (the
    // rest is model latency). Logging it makes a pickup regression visible in
    // `wrangler tail` instead of only in the dashboard feeling.
    const queuedAt = Date.parse(String(claim.message?.created_at || ''))
    if (Number.isFinite(queuedAt)) {
      console.log({
        event: 'run.stage',
        run_id: claim.run_id,
        stage: 'queue_to_claim',
        duration_ms: Date.now() - queuedAt,
      })
    }
    await runTurn(budget, reservation, item.agent_id, claim)
    return true
  } catch (error) {
    console.error(`claim failed for message ${item.message.id}:`, error instanceof Error ? error.message : error)
    return true
  } finally {
    reservation.release()
    releaseTurn(lane)
  }
}

async function claimTurn(
  budget: SubrequestBudget,
  agentId: string,
  messageId: number,
  claimToken: string,
  expectedRunId: string,
  signal?: AbortSignal,
): Promise<Claim | null> {
  const response = await agentFetch(`/agents/${agentId}/inbox/${messageId}/claim`, {
    method: 'POST',
    headers: { 'X-Claim-Token': claimToken },
    body: '{}',
    signal,
  }, budget)
  if (response.status === 404) return null
  if (response.status === 409) throw new ClaimInProgressError()
  if (!response.ok) throw new Error(`claim returned ${response.status}`)
  const payload: any = await response.json()
  const claim = payload?.data as Claim | undefined
  if (!claim?.run_id || claim.run_id !== expectedRunId) {
    throw new Error('claim run binding mismatch')
  }
  return claim
}

class ClaimInProgressError extends Error {
  constructor() {
    super('run is already claimed')
  }
}

/**
 * A finished turn: the terminal payload it reported and whether the backend
 * acknowledged it. Re-running the turn to resend an unacknowledged report would
 * duplicate paid model/tool work, so the outcome is returned instead of thrown.
 */
interface TurnResult {
  outcome: RunReportOutcome
  report: TerminalRunReport
}

async function runTurn(
  budget: SubrequestBudget,
  reservation: RunTerminalReservation,
  agentId: string,
  claim: Claim,
  parentSignal?: AbortSignal,
): Promise<TurnResult> {
  const events = new EventStream(budget, reservation, agentId, claim.run_id, claim.conversation_id)
  const started = Date.now()
  const controller = new AbortController()
  const abortFromParent = () => controller.abort(parentSignal?.reason || new Error('Wake request disconnected'))
  if (parentSignal?.aborted) abortFromParent()
  else parentSignal?.addEventListener('abort', abortFromParent, { once: true })
  const timeout = setTimeout(() => controller.abort(new Error('Turn exceeded the time budget')), TURN_TIMEOUT_MS)
  try {
    await events.pushAndFlush('message.start', '', controller.signal)
    const output = agentEngine === 'builtin'
      ? await executeTurnBuiltin(budget, agentId, claim, events, controller.signal)
      : await executeTurnPi(agentId, claim, events, piEngineDeps(budget), controller.signal)
    // The final batch spends this run's optional terminal slot, the report its
    // reserved PATCH slot: a completed answer lands even when streaming ran out.
    await events.pushAndFlush('message.final', output, controller.signal, 'terminal')
    const report: TerminalRunReport = { status: 'succeeded', content: output, output }
    const outcome = await finishRun(budget, reservation, agentId, claim.run_id, report, controller.signal)
    if (outcome === 'delivered') {
      console.log(`run ${claim.run_id} succeeded in ${Date.now() - started}ms`)
    } else {
      // The paid work is done. The caller keeps the report durably and the next
      // wake replays it, so a retry never re-enters the model turn.
      console.warn({
        event: 'run.stage', run_id: claim.run_id, stage: 'terminal_report_unconfirmed',
        outcome, subrequests_remaining: budget.remaining(),
      })
    }
    return { outcome, report }
  } catch (error) {
    // A client disconnect/cancel is a transport failure, not a terminal agent
    // outcome. Leave the backend run claim intact and let the durable dispatch
    // retry the same idempotent event instead of recording a false failure.
    if (parentSignal?.aborted) throw error
    const message = error instanceof Error ? error.message : 'Unexpected worker failure'
    const code = error instanceof ApiError && error.errorCode ? error.errorCode : 'WORKER_ERROR'
    console.error(`run ${claim.run_id} failed:`, message)
    await events.pushAndFlush('error', message, undefined, 'terminal').catch(() => {})
    const report: TerminalRunReport = { status: 'failed', error_code: code, error_message: message }
    const outcome = await finishRun(budget, reservation, agentId, claim.run_id, report)
    if (outcome !== 'delivered') {
      console.warn({
        event: 'run.stage', run_id: claim.run_id, stage: 'terminal_report_unconfirmed',
        outcome, status: 'failed', subrequests_remaining: budget.remaining(),
      })
    }
    return { outcome, report }
  } finally {
    clearTimeout(timeout)
    parentSignal?.removeEventListener('abort', abortFromParent)
  }
}

// Engine wiring: MAX_STEPS is resolved here with the same expression the
// builtin engine uses, so both engines honor the env var identically.
function piEngineDeps(budget: SubrequestBudget): PiEngineDeps {
  return {
    connectMcp: (agentId, signal) => connectMcp(agentId, budget, signal),
    maxSteps: Math.max(1, Number(maxStepsEnv || 8)),
    budget,
  }
}

async function executeTurnBuiltin(
  budget: SubrequestBudget,
  agentId: string,
  claim: Claim,
  events: EventStream,
  signal?: AbortSignal,
): Promise<string> {
  const model = claim.model
  if (!model) throw new ApiError(0, 'Agent config has no model', 'CONFIG_INVALID')
  const protocol = resolveModelProtocol(claim)
  const mcpStartedAt = Date.now()
  const mcpEnabled = shouldConnectMcp(claim)
  const mcp = mcpEnabled ? await connectMcp(agentId, budget, signal) : null
  console.log({
    event: 'run.stage',
    run_id: claim.run_id,
    stage: 'mcp_preflight',
    duration_ms: Date.now() - mcpStartedAt,
    enabled: mcpEnabled,
  })
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
    const result = await callModel(budget, model, messages, tools, async (text) => {
      await events.pushDelta(text, signal)
    }, claim.model_base_url, protocol, signal, claim.run_id)
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
        await events.pushAndFlush('tool.call', JSON.stringify({ name: call.name, arguments: safeJson(call.arguments) }), signal)
        toolOutput = await mcp.call(call.name, safeJson(call.arguments))
        await events.pushAndFlush('tool.result', JSON.stringify({ name: call.name, summary: summarize(toolOutput) }), signal)
      } catch (error) {
        if (signal?.aborted) throw abortError(signal)
        const message = error instanceof Error ? error.message : 'Tool failed'
        await events.pushAndFlush('tool.result', JSON.stringify({ name: call.name, error: message }), signal)
        toolOutput = `Tool error: ${message}`
      }
      messages.push({ role: 'tool', tool_call_id: call.id || `call_${step}_${call.name}`, content: toolOutput })
    }
    finalContent = ''
  }

  await events.flush('bestEffort', signal)
  return finalContent
}

/**
 * Sends the terminal run report from the run's own reserved PATCH slots. An
 * ambiguous result (no response, 408, 429, 5xx) is retried once — the PATCH is
 * idempotent and carries the answer, so resending it never re-runs the model or
 * tool work. `rejected` (409 nonterminal conflict, 404, 400) is returned as-is
 * instead of throwing: it is not an acknowledgement, but turning it into a
 * generic error made the caller retry it as ambiguous. Only an acknowledged
 * report may close the run's wake event.
 */
async function finishRun(
  budget: SubrequestBudget,
  reservation: RunTerminalReservation,
  agentId: string,
  runId: string,
  report: TerminalRunReport,
  signal?: AbortSignal,
): Promise<RunReportOutcome> {
  for (let attempt = 0; attempt < REPORT_ATTEMPTS_PER_RUN; attempt++) {
    try {
      const response = await agentFetch(`/agents/${agentId}/runs/${runId}`, {
        method: 'PATCH',
        body: JSON.stringify(report),
        signal,
      }, budget, () => reservation.takeReport())
      const outcome = classifyRunReport(response.status)
      if (outcome === 'delivered') return 'delivered'
      if (outcome === 'rejected') return 'rejected'
      // Ambiguous: retry the idempotent report with this run's remaining slot.
    } catch (error) {
      if (error instanceof SubrequestBudgetExhaustedError || isSubrequestLimitError(error)) return 'unsent'
      if (error instanceof ApiError) throw error
      if (signal?.aborted) throw abortError(signal)
      // No response at all: the report may or may not have landed. Retrying the
      // idempotent PATCH (never the turn) is safe.
    }
  }
  return 'ambiguous'
}

async function heartbeat(trace: RuntimeTrace, budget: SubrequestBudget): Promise<void> {
  await traceRegistration(trace, apiUrl, workerToken, 'heartbeat', { version: WORKER_VERSION }, budget)
}

// Hour-bucket idempotency key per agent: a retried cron within the same hour
// returns the existing queued turn instead of duplicating it. Legacy fallback
// only: an agent whose schedules are owned by its Durable Object must NOT also
// be enqueued from cron (two schedulers for one logical task).
async function queueScheduledRuns(agents: WorkerAgent[], budget: SubrequestBudget): Promise<void> {
  const hourBucket = new Date().toISOString().slice(0, 13)
  for (const agent of agents) {
    try {
      await agentFetch(`/agents/${agent.id}/scheduled-runs`, {
        method: 'POST',
        body: JSON.stringify({ idempotency_key: `${agent.id}:${hourBucket}` }),
      }, budget)
    } catch (error) {
      console.error(`scheduled run for agent ${agent.id} failed:`, error instanceof Error ? error.message : error)
    }
  }
}

/** Agent roster for this workspace Worker token; never throws. */
async function listWorkerAgents(budget: SubrequestBudget): Promise<WorkerAgent[]> {
  // Preferred: the scoped deployment config. It carries the workspace id the
  // canonical Durable Object name needs and returns only this token's workspace.
  try {
    const response = await agentFetch('/deployment/config', { method: 'GET' }, budget)
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
    const response = await agentFetch('/agents', { method: 'GET' }, budget)
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
async function handshake(trace: RuntimeTrace, budget: SubrequestBudget): Promise<void> {
  const wake = wakeCallback()
  const wakeSecret = String(runtimeEnv?.WAKE_SECRET || '').trim()
  await traceRegistration(trace, apiUrl, workerToken, 'handshake', {
    runtime_version: WORKER_VERSION, nonce: crypto.randomUUID(),
    ...(isWakeConfigured() && wake.url ? { wake_url: wake.url, wake_secret: wakeSecret } : {}),
  }, budget)
}

/**
 * Resends a finished turn's unacknowledged terminal report and reports whether
 * the backend acknowledged it. The paid model/tool work is never re-run: only
 * the idempotent PATCH is retried.
 */
async function replayPendingReport(
  budget: SubrequestBudget,
  reservation: RunTerminalReservation,
  wake: WakePayload,
  pending: PendingRunReport,
  signal?: AbortSignal,
): Promise<RunReportOutcome> {
  try {
    return await finishRun(budget, reservation, wake.agent_id, wake.run_id, toTerminalReport(pending), signal)
  } catch (error) {
    console.error('pending report replay failed:', error instanceof Error ? error.message : error)
    return 'unsent'
  }
}

async function handleWake(request: Request, budget: SubrequestBudget): Promise<Response> {
  const secret = String(runtimeEnv?.WAKE_SECRET || '').trim()
  const namespace = runtimeEnv?.WORKSPACE_AGENT
  if (!secret || !namespace) {
    return Response.json({ ok: false, error: 'WAKE_NOT_CONFIGURED' }, { status: 503 })
  }
  const contentLength = Number(request.headers.get('content-length') || 0)
  if (contentLength > 8192) return Response.json({ ok: false, error: 'BODY_TOO_LARGE' }, { status: 413 })
  let raw: Uint8Array
  try {
    raw = await readBoundedRequestBody(request, 8192)
  } catch (error) {
    if (error instanceof RequestBodyTooLargeError) {
      return Response.json({ ok: false, error: 'BODY_TOO_LARGE' }, { status: 413 })
    }
    return Response.json({ ok: false, error: 'INVALID_WAKE_BODY' }, { status: 400 })
  }

  let binding
  try {
    binding = await verifyWakeSignature(request, raw, secret)
  } catch (error) {
    const status = error instanceof WakeAuthError ? error.status : 401
    return Response.json({ ok: false, error: 'INVALID_WAKE_SIGNATURE' }, { status })
  }

  let wake: WakePayload
  try {
    wake = JSON.parse(new TextDecoder().decode(raw)) as WakePayload
  } catch {
    return Response.json({ ok: false, error: 'INVALID_WAKE_BODY' }, { status: 400 })
  }
  if (
    !wake?.event_id || !wake?.run_id || !wake?.agent_id || !Number.isSafeInteger(wake?.message_id) ||
    wake.deployment_id !== binding.deploymentId || wake.workspace_id !== binding.workspaceId
  ) {
    return Response.json({ ok: false, error: 'WAKE_BINDING_MISMATCH' }, { status: 403 })
  }
  // The lane is signed by the backend with the rest of the wake payload; the
  // Worker never chooses it. It decides which capacity pool this turn uses.
  const wakeLane = normalizeLane(wake.lane)
  if (!tryReserveTurn(wakeLane)) {
    return Response.json({ ok: false, error: 'TURN_CAPACITY_EXHAUSTED' }, { status: 429, headers: { 'Retry-After': '2' } })
  }

  let turnSlotTransferred = false
  let reservation: RunTerminalReservation | null = null
  try {

  // The report is reserved before the wake is authorized or the turn claimed:
  // this invocation either pays for the run's terminal state or claims nothing.
  reservation = budget.reserveRun(wake.run_id)
  if (!reservation) {
    return Response.json({ ok: false, error: 'SUBREQUEST_BUDGET_EXHAUSTED' }, { status: 503, headers: { 'Retry-After': '2' } })
  }
  // The authorization RPC gates the work, so it is charged as essential while
  // the run's terminal slots stay reserved. Completion/release are bookkeeping
  // after the terminal state and are never charged (they must not mask a
  // delivered report).
  if (!budget.take('essential')) {
    return Response.json({ ok: false, error: 'SUBREQUEST_BUDGET_EXHAUSTED' }, { status: 503, headers: { 'Retry-After': '2' } })
  }

  const object = namespace.getByName(durableObjectName(wake.workspace_id, wake.agent_id))
  const authorization = await object.authorizeWake({
    workspace_id: wake.workspace_id,
    agent_id: wake.agent_id,
    event_id: wake.event_id,
    nonce: binding.nonce,
    now: Date.now(),
  })
  if (authorization.rate_limited) {
    return Response.json({ ok: false, error: 'WAKE_RATE_LIMITED' }, { status: 429, headers: { 'Retry-After': '60' } })
  }
  if (!authorization.accepted) {
    if (authorization.duplicate_completed) {
      return Response.json({ ok: true, duplicate: true, run_id: wake.run_id })
    }
    if (authorization.duplicate) {
      return Response.json({ ok: false, error: 'WAKE_IN_PROGRESS' }, { status: 409, headers: { 'Retry-After': '2' } })
    }
    return Response.json({ ok: false, error: 'WAKE_WORKSPACE_MISMATCH' }, { status: 403 })
  }

  // A turn that already finished keeps its terminal report in the object when
  // the backend never acknowledged it. Replaying that report is idempotent and
  // never re-runs the paid model/tool work, which a re-claim would: the backend
  // resumes a claim for the same event token.
  const pending = await object.getPendingReport(wake.run_id)
  if (pending) {
    const outcome = await replayPendingReport(budget, reservation, wake, pending, request.signal)
    if (outcome === 'delivered') {
      await object.clearPendingReport(wake.run_id)
      await object.completeWake(wake.event_id)
      // Same acknowledgement shape as a duplicate wake: no turn ran here, and
      // the run row is terminal now.
      return Response.json({ ok: true, duplicate: true, run_id: wake.run_id })
    }
    await object.releaseWake(wake.event_id).catch(() => undefined)
    return Response.json(
      { ok: false, error: 'WAKE_REPORT_UNCONFIRMED', run_id: wake.run_id },
      { status: 503, headers: { 'Retry-After': '2' } },
    )
  }

  let claim: Claim | null
  try {
    claim = await claimTurn(budget, wake.agent_id, wake.message_id, wake.event_id, wake.run_id, request.signal)
  } catch (error) {
    await object.releaseWake(wake.event_id)
    console.error('wake claim failed:', error instanceof Error ? error.message : error)
    if (error instanceof ClaimInProgressError) {
      return Response.json({ ok: false, error: 'WAKE_IN_PROGRESS', run_id: wake.run_id }, { status: 409, headers: { 'Retry-After': '2' } })
    }
    return Response.json({ ok: false, error: 'WAKE_CLAIM_FAILED' }, { status: 502 })
  }
  if (!claim) {
    await object.releaseWake(wake.event_id)
    return Response.json({ ok: false, error: 'WAKE_CLAIM_NOT_FOUND', run_id: wake.run_id }, { status: 409, headers: { 'Retry-After': '2' } })
  }

  const turnController = new AbortController()
  const abortTurn = () => {
    if (!turnController.signal.aborted) {
      turnController.abort(request.signal.reason || new Error('Wake request disconnected'))
    }
  }
  if (request.signal.aborted) abortTurn()
  else request.signal.addEventListener('abort', abortTurn, { once: true })
  let streamClosed = false
  let heartbeat: ReturnType<typeof setInterval> | undefined
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      const encode = (value: unknown) => new TextEncoder().encode(`${JSON.stringify(value)}\n`)
      const enqueue = (value: unknown): boolean => {
        if (streamClosed) return false
        try {
          controller.enqueue(encode(value))
          return true
        } catch {
          streamClosed = true
          abortTurn()
          return false
        }
      }
      enqueue({ type: 'accepted', event_id: wake.event_id })
      heartbeat = setInterval(() => enqueue({ type: 'heartbeat' }), 15_000)
      void (async () => {
        try {
          const turn = await runTurn(budget, reservation!, wake.agent_id, claim, turnController.signal)
          if (shouldCompleteWake(turn.outcome)) {
            await object.completeWake(wake.event_id)
            enqueue({ type: 'complete', run_id: wake.run_id })
            return
          }
          // The paid work is done but its report was not acknowledged. Keep the
          // payload so the retried wake replays it instead of re-running the
          // turn, and leave the event open: it is not committed.
          await object.recordPendingReport(wake.run_id, turn.report)
          await object.releaseWake(wake.event_id)
          enqueue({ type: 'error' })
        } catch (error) {
          await object.releaseWake(wake.event_id).catch(() => {})
          enqueue({ type: 'error' })
        } finally {
          reservation?.release()
          releaseTurn(wakeLane)
          if (heartbeat) clearInterval(heartbeat)
          request.signal.removeEventListener('abort', abortTurn)
          if (!streamClosed) {
            streamClosed = true
            controller.close()
          }
        }
      })()
    },
    cancel(reason) {
      streamClosed = true
      if (heartbeat) clearInterval(heartbeat)
      if (!turnController.signal.aborted) {
        turnController.abort(reason instanceof Error ? reason : new Error('Wake response cancelled'))
      }
    },
  })
  turnSlotTransferred = true
  return new Response(stream, {
    status: 200,
    headers: { 'Content-Type': 'application/x-ndjson', 'Cache-Control': 'no-store' },
  })
  } finally {
    if (!turnSlotTransferred) {
      // Setup failed before the stream took ownership of the run's slots.
      reservation?.release()
      releaseTurn(wakeLane)
    }
  }
}

/**
 * Desired schedules for one agent, as owned by the backend (`agent_schedules`).
 * An absent endpoint (older backend) is a no-op, not an error: DO sync turns on
 * through AGENT_DO_SYNC only after this contract ships.
 */
async function fetchDesiredSchedules(agentId: string, budget: SubrequestBudget): Promise<DesiredSchedule[] | null> {
  try {
    const response = await agentFetch(`/agents/${agentId}/schedules`, { method: 'GET' }, budget)
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
 * Returns the agent ids whose scheduling is now owned by a Durable Object.
 * A successful empty schedule sync is still ownership: it deliberately means
 * "no recurring work" and must never fall through to the legacy hourly run.
 */
async function syncAgentDurableObjects(agents: WorkerAgent[], budget: SubrequestBudget): Promise<Set<string>> {
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
      const desired = await fetchDesiredSchedules(agent.id, budget)
      if (!scheduleSyncOwnsAgent(desired)) continue
      const stub = namespace.getByName(durableObjectName(workspaceId, agent.id))
      const result = await stub.syncSchedules(desired!, Number(agent.config_revision || 0))
      owned.add(agent.id)
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
        }, budget)
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

function isBaseConfigured(): boolean {
  return Boolean(workerToken) && Boolean(apiUrl) && !isPlaceholderToken(workerToken) &&
    Boolean(String(runtimeEnv?.MODEL_API_KEY || '').trim())
}

/** One resolver for handshake, config gates, and the diagnostic surface. */
function wakeCallback(): ResolvedWakeCallback {
  return resolveWakeCallbackUrl({
    baseUrl: runtimeEnv?.WORKER_BASE_URL,
    callbackUrl: runtimeEnv?.WAKE_CALLBACK_URL,
  })
}

function isWakeConfigured(): boolean {
  const wakeSecret = String(runtimeEnv?.WAKE_SECRET || '').trim()
  const wakeSecretBytes = new TextEncoder().encode(wakeSecret).byteLength
  return wakeSecretBytes >= 32 && wakeSecretBytes <= 256 && Boolean(wakeCallback().url) &&
    Boolean(runtimeEnv?.WORKSPACE_AGENT)
}
export default {
  async scheduled(event: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    applyEnv(env)
    // One budget for this invocation only, alive exactly as long as its
    // ctx.waitUntil work: a wake stream overlapping on this isolate has its own.
    const budget = createSubrequestBudget(env.SUBREQUEST_BUDGET)
    const trace = runtimeTrace(env, WORKER_VERSION, 'scheduled')
    logRuntimeTrace(trace, 'runtime.invocation.start', {
      cron: event.cron, scheduled_at: new Date(event.scheduledTime).toISOString(),
      schedule_lag_ms: Date.now() - event.scheduledTime,
    })
    if (!isBaseConfigured()) {
      logRuntimeTrace(trace, 'runtime.configuration.invalid', {}, true)
      console.error('Worker is not configured: set URL_BACKEND, WORKER_TOKEN, and MODEL_API_KEY')
      return
    }
    ctx.waitUntil((async () => {
      // Registration is best-effort during a staged rollout. The public wake
      // route itself stays fail-closed, while existing outbound cron recovery
      // continues if callback settings are absent or the handshake is down.
      const hasWakeSettings = Boolean(
        String(env.WORKER_BASE_URL || '').trim() || String(env.WAKE_CALLBACK_URL || '').trim() ||
        String(env.WAKE_SECRET || '').trim(),
      )
      if (hasWakeSettings && !isWakeConfigured()) {
        // An invalid WORKER_BASE_URL never falls back to WAKE_CALLBACK_URL, so
        // name the missing piece instead of reporting a generic misconfiguration.
        console.error(
          wakeCallback().url
            ? 'realtime wake is disabled: 32-256 byte WAKE_SECRET and WORKSPACE_AGENT must both be valid'
            : 'realtime wake is disabled: WORKER_BASE_URL (or legacy WAKE_CALLBACK_URL) must be a valid HTTPS origin; an invalid WORKER_BASE_URL does not fall back',
        )
      }
      try {
        await handshake(trace, budget)
      } catch (error) {
        console.error('worker handshake failed; continuing cron recovery:', error instanceof Error ? error.message : error)
      }
      const drain = drainInbox(budget)
      await Promise.all([
        heartbeat(trace, budget),
        (async () => {
          const agents = await listWorkerAgents(budget)
          if (doSyncEnabled) {
            // Every successfully synced agent is DO-owned, including an agent
            // whose desired schedule list is empty.
            const owned = await syncAgentDurableObjects(agents, budget)
            await queueScheduledRuns(agents.filter((agent) => !owned.has(agent.id)), budget)
            return
          }
          await queueScheduledRuns(agents, budget)
        })(),
      ])
      await drain
      logRuntimeTrace(trace, 'runtime.invocation.complete', {
        subrequests_used: budget.spent,
        subrequests_limit: budget.limit,
        subrequests_redirects: budget.redirectsSeen,
        subrequests_reserved: budget.reserved,
      })
    })().catch((error) => {
      logRuntimeTrace(trace, 'runtime.invocation.failed', { failure: 'runtime_task_failure' }, true)
      throw error
    }))
  },

  // GET / is a health probe. The only paid-work ingress is the authenticated
  // POST /v1/wake route; the former unauthenticated POST / drain is closed.
  async fetch(request: Request, env: Env, _ctx: ExecutionContext): Promise<Response> {
    applyEnv(env)
    const trace = runtimeTrace(env, WORKER_VERSION, 'fetch')
    logRuntimeTrace(trace, 'runtime.invocation.start', { method: request.method })
    const requestURL = new URL(request.url)
    const route = classifyRuntimeRoute(request.method, requestURL.pathname, requestURL.search)
    if (route === 'method_not_allowed') {
      return Response.json({ ok: false, error: 'METHOD_NOT_ALLOWED' }, { status: 405, headers: { Allow: 'POST' } })
    }
    if (route === 'not_found') {
      return Response.json({ ok: false, error: 'NOT_FOUND' }, { status: 404 })
    }
    const baseConfigured = isBaseConfigured()
    const wakeConfigured = isWakeConfigured()
    const wake = wakeCallback()
    if (route === 'wake') {
      if (!baseConfigured || !wakeConfigured) {
        logRuntimeTrace(trace, 'runtime.configuration.invalid', {}, true)
        return Response.json({ ok: false, error: 'WAKE_NOT_CONFIGURED' }, { status: 503 })
      }
      // Execution stays attached to the streaming request lifetime. It must
      // never move to waitUntil after an early 202 response. The budget lives
      // exactly as long as this request/stream, never shared with cron.
      return await handleWake(request, createSubrequestBudget(env.SUBREQUEST_BUDGET))
    }
    // Health is read-only: it never heartbeats, drains, claims, or starts a
    // model turn. Cron/handshake own registration state changes.
    return Response.json({
      ok: true,
      version: WORKER_VERSION,
      environment: String(env.ENVIRONMENT || '').toLowerCase() === 'development' ? 'development' : 'production',
      backend: apiUrl,
      token_configured: !isPlaceholderToken(workerToken),
      configured: baseConfigured,
      wake_configured: wakeConfigured,
      wake_url_source: wake.source,
    })
  },
}
