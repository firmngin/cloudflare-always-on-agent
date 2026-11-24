/**
 * Named Durable Object per agent — the durable lifecycle + scheduler core the
 * product plan (v1.2 §2.1, §6.1, §9.3) requires.
 *
 * One object per `(workspace_id, agent_id)`, addressed by
 * `w:{workspace_id}:a:{agent_id}` through `getByName()`. Its ONLY jobs are:
 *   - durable identity + tiny state (applied schedule version, last wake),
 *   - the recurring schedules (Cloudflare Agents `Scheduler`, SQLite-backed),
 *   - turning a fired schedule into an idempotent enqueue call to Firmngin.
 *
 * It deliberately does NOT hold chat history, long-poll, or the model call:
 * those stay on the run queue + Worker engine (plan §2.1 "Tidak menangani:
 * ... long polling, heavy LLM wait", §6.4). A scheduler callback must be short
 * and idempotent because Cloudflare may replay it after eviction.
 *
 * Enablement: the Worker only syncs objects when `AGENT_DO_SYNC="true"` so a
 * deployment that has not moved its schedules yet keeps working unchanged.
 */

import { DurableObject } from 'cloudflare:workers'
import { Lifecycle } from 'agents/lifecycle'
import { Scheduler, type Schedule } from 'agents/schedules'
import { resolveBackendUrl, runtimeFetch, type BackendEnvLike } from '../runtime/endpoints'
import {
  clearPendingReportState,
  pendingReportFor,
  recordPendingReportState,
  type PendingReportState,
  type PendingRunReport,
} from '../runtime/pending-report'
import type { TerminalRunReport } from '../runtime/turn'
import {
  authorizeWakeState,
  completeWakeState,
  releaseWakeState,
  type WakeSecurityState,
} from '../runtime/wake-security'

export interface WorkspaceAgentEnv extends BackendEnvLike {
  WORKER_TOKEN: string
}

/** One desired schedule as owned by the backend (`agent_schedules`). */
export interface DesiredSchedule {
  schedule_id: string
  name: string
  kind: 'cron' | 'interval'
  /** UTC cron expression (the backend converts from the authored timezone). */
  cron?: string
  interval_seconds?: number
  payload?: Record<string, unknown>
  enabled: boolean
  desired_version: number
}

export interface ScheduleSyncResult {
  agent_id: string
  applied_version: number
  applied_config_version: number
  created: number
  cancelled: number
  unchanged: number
}

export interface RuntimeState {
  object_name: string
  agent_id: string
  workspace_id: string
  applied_version: number
  applied_config_version: number
  last_wake_at: string | null
  schedules: Array<{ id: string, schedule_id: string, type: string, next_time: number | null }>
}

interface DueRunPayload extends Record<string, unknown> {
  workspace_id: string
  agent_id: string
  schedule_id: string
  schedule_name?: string
}

const APPLIED_VERSION_KEY = 'applied_version'
const APPLIED_CONFIG_VERSION_KEY = 'applied_config_version'
const LAST_WAKE_KEY = 'last_wake_at'
const WAKE_SECURITY_KEY = 'wake_security_v1'
const PENDING_REPORTS_KEY = 'pending_reports_v1'

export function durableObjectName(workspaceID: string, agentID: string): string {
  return `w:${workspaceID}:a:${agentID}`
}

export class WorkspaceAgent extends DurableObject<WorkspaceAgentEnv> {
  /**
   * Callback names are registered unconditionally: a schedule stores the name,
   * not the function, so the map must exist after an eviction wake-up.
   */
  readonly scheduler = new Scheduler({
    callbacks: {
      enqueueDueRun: (payload: DueRunPayload, schedule: Schedule<DueRunPayload>) => this.enqueueDueRun(payload, schedule),
    },
    retry: { maxAttempts: 3, baseDelayMs: 250, maxDelayMs: 4000 },
    onError: (error) => {
      // Never fail the object: a lost enqueue is recovered by the next
      // occurrence and by the backend's stale-run reconciliation.
      console.error('scheduled enqueue failed:', error instanceof Error ? error.message : error)
    },
  })

  readonly lifecycle = Lifecycle.install(this).use(this.scheduler)

  /** Durable replay/rate/event gate for authenticated realtime wakes. */
  async authorizeWake(input: {
    workspace_id: string
    agent_id: string
    event_id: string
    nonce: string
    now: number
  }): Promise<{ accepted: boolean, duplicate: boolean, duplicate_completed: boolean, rate_limited: boolean }> {
    const identity = this.identity()
    if (input.workspace_id !== identity.workspaceID || input.agent_id !== identity.agentID) {
      return { accepted: false, duplicate: false, duplicate_completed: false, rate_limited: false }
    }
    return await this.ctx.storage.transaction(async (txn) => {
      const current = await txn.get<WakeSecurityState>(WAKE_SECURITY_KEY)
      const decision = authorizeWakeState(current, input.nonce, input.event_id, input.now)
      await txn.put(WAKE_SECURITY_KEY, decision.state)
      return {
        accepted: decision.accepted,
        duplicate: decision.duplicate,
        duplicate_completed: decision.duplicateCompleted,
        rate_limited: decision.rateLimited,
      }
    })
  }

  async completeWake(eventId: string): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const current = (await txn.get<WakeSecurityState>(WAKE_SECURITY_KEY)) || { nonces: {}, events: {}, requests: [] }
      await txn.put(WAKE_SECURITY_KEY, completeWakeState(current, eventId, Date.now()))
    })
  }

  async releaseWake(eventId: string): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const current = (await txn.get<WakeSecurityState>(WAKE_SECURITY_KEY)) || { nonces: {}, events: {}, requests: [] }
      await txn.put(WAKE_SECURITY_KEY, releaseWakeState(current, eventId))
    })
  }

  /**
   * Durable fence for a turn that finished but whose terminal report was never
   * acknowledged. The next wake for this run replays the report instead of
   * re-entering the turn, so a retry cannot re-spend the user's model key.
   */
  async getPendingReport(runId: string): Promise<PendingRunReport | null> {
    const state = await this.ctx.storage.get<PendingReportState>(PENDING_REPORTS_KEY)
    return pendingReportFor(state, runId, Date.now())
  }

  async recordPendingReport(runId: string, report: TerminalRunReport): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const current = await txn.get<PendingReportState>(PENDING_REPORTS_KEY)
      await txn.put(PENDING_REPORTS_KEY, recordPendingReportState(current, runId, report, Date.now()))
    })
  }

  async clearPendingReport(runId: string): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const current = await txn.get<PendingReportState>(PENDING_REPORTS_KEY)
      await txn.put(PENDING_REPORTS_KEY, clearPendingReportState(current, runId, Date.now()))
    })
  }

  /**
   * Replaces this agent's recurring schedules with the backend's desired state.
   * Idempotent: a no-op sync creates and cancels nothing. Unrelated schedules
   * on the object are never touched (they would be a different callback).
   */
  async syncSchedules(desired: DesiredSchedule[], configRevision = 0): Promise<ScheduleSyncResult> {
    await this.lifecycle.start()
    const identity = this.identity()

    const wanted = new Map<string, DesiredSchedule>()
    for (const item of desired) {
      if (!item?.schedule_id) continue
      if (item.enabled) wanted.set(item.schedule_id, item)
    }

    const existing = await this.scheduler.list()
    const existingByScheduleID = new Map<string, Schedule<any>>()
    let cancelled = 0
    for (const schedule of existing) {
      if (schedule.callback !== 'enqueueDueRun') continue
      const payload = (schedule.payload || {}) as DueRunPayload
      const scheduleID = String(payload.schedule_id || '')
      if (!scheduleID) continue
      const target = wanted.get(scheduleID)
      if (target && sameSignature(schedule, target)) {
        existingByScheduleID.set(scheduleID, schedule)
        continue
      }
      // Gone, disabled, or changed: drop it and re-create below when wanted.
      if (await this.scheduler.cancel(schedule.id)) cancelled++
    }

    let created = 0
    let unchanged = 0
    let skipped = 0
    for (const [scheduleID, target] of wanted) {
      if (existingByScheduleID.has(scheduleID)) {
        unchanged++
        continue
      }
      const payload: DueRunPayload = {
        workspace_id: identity.workspaceID,
        agent_id: identity.agentID,
        schedule_id: scheduleID,
        schedule_name: target.name,
        ...(target.payload || {}),
      }
      const options = { idempotent: true }
      try {
        if (target.kind === 'interval') {
          const seconds = Math.max(1, Math.floor(Number(target.interval_seconds || 0)))
          await this.scheduler.every(seconds, 'enqueueDueRun', payload, options)
        } else {
          const cron = String(target.cron || '').trim()
          if (!cron) continue
          await this.scheduler.set(cron, 'enqueueDueRun', payload, options)
        }
      } catch (error) {
        // One unparseable expression must not abort the whole sync: the other
        // schedules still converge, and the backend's authoring validation is
        // what keeps this from happening (see normalizeAgentScheduleCron).
        console.error(
          `schedule ${scheduleID} skipped: ${error instanceof Error ? error.message : error}`,
        )
        skipped++
        continue
      }
      created++
    }

    const appliedVersion = desired.reduce((max, item) => Math.max(max, Number(item.desired_version || 0)), 0)
    const appliedConfigVersion = Math.max(0, Math.floor(Number(configRevision) || 0))
    await this.ctx.storage.put(APPLIED_VERSION_KEY, appliedVersion)
    await this.ctx.storage.put(APPLIED_CONFIG_VERSION_KEY, appliedConfigVersion)

    if (skipped) {
      console.error(`${skipped} schedule(s) could not be applied`)
    }

    return {
      agent_id: identity.agentID,
      applied_version: appliedVersion,
      applied_config_version: appliedConfigVersion,
      created,
      cancelled,
      unchanged,
    }
  }

  /** Small durable state for the backend's `agent_runtime_bindings` row. */
  async getRuntimeState(): Promise<RuntimeState> {
    await this.lifecycle.start()
    const identity = this.identity()
    const appliedVersion = Number((await this.ctx.storage.get<number>(APPLIED_VERSION_KEY)) || 0)
    const appliedConfigVersion = Number((await this.ctx.storage.get<number>(APPLIED_CONFIG_VERSION_KEY)) || 0)
    const lastWakeAt = (await this.ctx.storage.get<string>(LAST_WAKE_KEY)) || null
    const schedules = (await this.scheduler.list())
      .filter((schedule) => schedule.callback === 'enqueueDueRun')
      .map((schedule) => ({
        id: schedule.id,
        schedule_id: String((schedule.payload as DueRunPayload | undefined)?.schedule_id || ''),
        type: String(schedule.type || ''),
        next_time: typeof schedule.time === 'number' ? schedule.time : null,
      }))
    return {
      object_name: this.lifecycle.name || '',
      agent_id: identity.agentID,
      workspace_id: identity.workspaceID,
      applied_version: appliedVersion,
      applied_config_version: appliedConfigVersion,
      last_wake_at: lastWakeAt,
      schedules,
    }
  }

  // -------------------------------------------------------------------------
  // Schedule callback: enqueue only. The backend owns run creation + dedupe.
  // -------------------------------------------------------------------------

  private async enqueueDueRun(payload: DueRunPayload, schedule: Schedule<DueRunPayload>): Promise<void> {
    const agentID = String(payload?.agent_id || this.identity().agentID)
    const scheduleID = String(payload?.schedule_id || '')
    if (!agentID || !scheduleID) return

    const apiUrl = resolveBackendUrl(this.env)
    const token = String(this.env.WORKER_TOKEN || '')
    if (!apiUrl || !token) return

    // The occurrence timestamp must be stable across a retried callback so the
    // backend can collapse replays into one run. The scheduler's stored time is
    // the occurrence it just fired; when it is not plausible, fall back to the
    // current minute bucket (cron precision). The backend additionally rejects
    // any occurrence at or before its own `last_occurrence_at`.
    const occurrence = occurrenceAt(schedule)

    const result = await runtimeFetch(apiUrl, token, `/agents/${agentID}/scheduled-runs`, {
      method: 'POST',
      body: JSON.stringify({ schedule_id: scheduleID, occurrence_at: occurrence }),
    })
    await this.ctx.storage.put(LAST_WAKE_KEY, new Date().toISOString())

    if (!result.ok) {
      // 409/429 are expected (duplicate occurrence, agent busy/paused); only a
      // 401/403 means the credential is gone and must surface loudly.
      if (result.status === 401 || result.status === 403) {
        console.error('scheduled enqueue rejected:', result.status, JSON.stringify(result.body ?? null).slice(0, 200))
      }
      return
    }
    console.log(`scheduled enqueue accepted: agent=${agentID} schedule=${scheduleID} occurrence=${occurrence}`)
  }

  /**
   * Identity comes from the object name (`w:{workspace}:a:{agent}`), never from
   * user-supplied payload: Cloudflare reads it back as `ctx.id.name`.
   */
  private identity(): { workspaceID: string, agentID: string } {
    const name = String(this.lifecycle.name || '')
    const match = /^w:([^:]+):a:([^:]+)$/.exec(name)
    if (!match) return { workspaceID: '', agentID: '' }
    return { workspaceID: match[1], agentID: match[2] }
  }
}

function sameSignature(schedule: Schedule<any>, desired: DesiredSchedule): boolean {
  const payload = (schedule.payload || {}) as DueRunPayload
  if (String(payload.schedule_name || '') !== String(desired.name || '')) return false
  if (desired.kind === 'interval') {
    if (schedule.type !== 'interval') return false
    return Number(schedule.intervalSeconds || 0) === Math.max(1, Math.floor(Number(desired.interval_seconds || 0)))
  }
  if (schedule.type !== 'cron') return false
  return String(schedule.cron || '') === String(desired.cron || '').trim()
}

function occurrenceAt(schedule: Schedule<any> | undefined): string {
  const now = Date.now()
  const stored = Number(schedule?.time || 0) * 1000
  if (Number.isFinite(stored) && Math.abs(now - stored) <= 30 * 60_000) {
    return new Date(stored).toISOString()
  }
  const minute = 60_000
  return new Date(Math.floor(now / minute) * minute).toISOString()
}
