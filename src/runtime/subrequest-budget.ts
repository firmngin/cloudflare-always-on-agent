/**
 * Per-invocation subrequest accounting for the Worker runtime.
 *
 * Cloudflare counts subrequests PER INVOCATION: 50 on Workers Free, 10,000 on
 * Workers Paid (`[limits] subrequests` raises the Paid cap). Each hop of a
 * redirect chain counts too, so the platform total can exceed the number of
 * `fetch()` calls in the code
 * (https://developers.cloudflare.com/workers/platform/limits/ — "Subrequests").
 * Once the cap is hit, EVERY later `fetch()` in that invocation throws "Too many
 * subrequests by single Worker invocation" — including the terminal run report
 * the dashboard is waiting for.
 *
 * Because the limit is per invocation, the budget is per invocation too:
 * `createSubrequestBudget()` is called once for the `scheduled()` invocation
 * (whose work runs inside its `ctx.waitUntil`) and once per `/v1/wake` request
 * (whose work runs for the streaming response's lifetime). There is no module
 * singleton and no idle reset, so a cron drain and a wake stream overlapping on
 * one isolate can neither refill nor poison each other's counter, and a wake
 * invocation is never refused work because a cron drain already spent its
 * budget.
 *
 * Inside one invocation, spending happens in three tiers:
 *   essential   - claims, model steps, MCP calls, inbox long-polls and wake
 *                 authorization: everything a claimed turn cannot finish without.
 *   bestEffort  - UI event batches and diagnostics: reduced first and coalesced
 *                 with a bounded size/time window, so a long answer streams a
 *                 few larger batches instead of starving the report.
 *   terminal    - NOT one shared pool. Each claimed run reserves its own report
 *                 attempts plus one optional final/error UI batch *before* the
 *                 claim (`reserveRun`). A cron invocation can run three turns at
 *                 once, so a shared reserve let three terminal flushes and their
 *                 retries exhaust it and stall a thread with no report at all.
 *                 With a per-run reservation, terminal UI flushes and retries
 *                 can never spend another run's PATCH headroom, and the report
 *                 keeps priority over the optional event batch.
 *
 * `REDIRECT_HEADROOM` stays unspent because a redirect hop is invisible to this
 * counter until the platform has already charged it. That is conservative, not
 * an absolute guarantee: if the deployment's real `[limits] subrequests` is
 * lower than `SUBREQUEST_BUDGET`, the runtime can still reach the platform cap —
 * at which point `markCapHit()` stops every further attempt in that invocation.
 */

/** Workers Free: 50 subrequests per invocation. */
export const DEFAULT_SUBREQUEST_BUDGET = 50
/** Headroom for model steps, MCP calls and claims once UI events stop. */
export const ESSENTIAL_HEADROOM = 12
/** Slots kept unspent because redirect hops are counted before we can see them. */
export const REDIRECT_HEADROOM = 2
/**
 * Terminal report (PATCH) attempts reserved per claimed run: the first report
 * and one idempotent retry when the response is ambiguous.
 */
export const REPORT_ATTEMPTS_PER_RUN = 2
/** Optional final/error UI batch reserved per claimed run (never a report slot). */
export const TERMINAL_EVENT_RESERVE_PER_RUN = 1
/** Total terminal headroom held for one claimed run. */
export const TERMINAL_RESERVE_PER_RUN = REPORT_ATTEMPTS_PER_RUN + TERMINAL_EVENT_RESERVE_PER_RUN

/** Event batch is flushed once it holds this many events ... */
export const EVENT_FLUSH_EVENTS = 32
/** ... or this many estimated payload bytes ... */
export const EVENT_FLUSH_BYTES = 4_096
/** ... or this long after the previous successful flush. */
export const EVENT_FLUSH_INTERVAL_MS = 300

/** Tiers the invocation-wide budget hands out directly. */
export type SubrequestTier = 'essential' | 'bestEffort'

/**
 * A fetch charge: one of the shared tiers, or a reservation slot callback
 * (`() => reservation.takeReport()` / `() => reservation.takeTerminalEvents()`).
 */
export type SubrequestCharge = SubrequestTier | (() => boolean)

export class SubrequestBudgetExhaustedError extends Error {}

/** The runtime's exact wording when an invocation runs out of subrequests. */
export function isSubrequestLimitError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error ?? '')
  return /too many subrequests/i.test(message)
}

/**
 * Terminal headroom held for ONE claimed run. Its slots are reserved from the
 * invocation budget before the claim and returned when the run stops, so an
 * overlapping run (or a later turn in the same drain) can reuse them.
 */
export class RunTerminalReservation {
  private reportAttempts = REPORT_ATTEMPTS_PER_RUN
  private terminalEvents = TERMINAL_EVENT_RESERVE_PER_RUN
  private released = false

  constructor(private readonly budget: SubrequestBudget, readonly runId: string) {}

  /** Slots still held for this run. */
  get held(): number {
    return this.released ? 0 : this.reportAttempts + this.terminalEvents
  }

  get reportAttemptsLeft(): number {
    return this.released ? 0 : this.reportAttempts
  }

  get terminalEventsLeft(): number {
    return this.released ? 0 : this.terminalEvents
  }

  /** One terminal report (PATCH) attempt from this run's own headroom. */
  takeReport(): boolean {
    if (this.released || this.reportAttempts <= 0 || !this.budget.chargeReserved()) return false
    this.reportAttempts--
    return true
  }

  /** The optional final/error UI batch; never the report's headroom. */
  takeTerminalEvents(): boolean {
    if (this.released || this.terminalEvents <= 0 || !this.budget.chargeReserved()) return false
    this.terminalEvents--
    return true
  }

  /** Returns unused slots; safe to call more than once (`finally` + error paths). */
  release(): void {
    if (this.released) return
    this.released = true
    this.reportAttempts = 0
    this.terminalEvents = 0
    this.budget.forgetRun(this.runId)
  }
}

/** The surface a turn needs from a budget: its own cap, the invocation's authority. */
export interface BudgetView {
  readonly spent: number
  readonly exhausted: boolean
  readonly redirectsSeen: number
  readonly reserved: number
  remaining(): number
  allows(tier: SubrequestTier): boolean
  take(tier: SubrequestTier): boolean
  reserveRun(runId: string): RunTerminalReservation | null
  /** Best-effort share for a turn, never more than this slice can spend. */
  bestEffortAllowance(slots: number): number
  /** Platform-level limit marker, forwarded to the invocation budget. */
  markCapHit(): void
  noteRedirect(): void
}

/**
 * One run's slice of the invocation budget.
 *
 * Without it every concurrent turn spent from one shared counter: a single long
 * streamed answer with many event flushes could leave the other turns without a
 * model step. The slice is a hard cap for this run, and every charge still goes
 * through the parent, so the invocation limit stays the outer bound.
 */
export class ScopedBudget implements BudgetView {
  private used = 0

  constructor(
    private readonly parent: BudgetView,
    readonly limit: number,
  ) {}

  get spent(): number { return this.used }
  get exhausted(): boolean { return this.parent.exhausted }
  get redirectsSeen(): number { return this.parent.redirectsSeen }
  get reserved(): number { return this.parent.reserved }

  remaining(): number {
    return Math.max(0, Math.min(this.limit - this.used, this.parent.remaining()))
  }

  allows(tier: SubrequestTier): boolean {
    if (this.used >= this.limit) return false
    return this.parent.allows(tier)
  }

  take(tier: SubrequestTier): boolean {
    if (this.used >= this.limit) return false
    if (!this.parent.take(tier)) return false
    this.used++
    return true
  }

  reserveRun(runId: string): RunTerminalReservation | null {
    // Terminal reports are reserved from the invocation pool, never from the
    // run's slice: the report must outlive the slice it reports on.
    return this.parent.reserveRun(runId)
  }

  bestEffortAllowance(slots: number): number {
    // Never more than this run can still spend: the slice is the hard cap and
    // the invocation-wide share is the outside bound.
    return Math.max(1, Math.min(this.remaining(), this.parent.bestEffortAllowance(slots)))
  }

  markCapHit(): void {
    // A platform rejection is an invocation-wide fact, not a per-run one.
    this.parent.markCapHit()
  }

  noteRedirect(): void {
    this.parent.noteRedirect()
  }
}

/**
 * Per-run slice of what is left.
 *
 * The divisor is the number of turns sharing it *now* (this one included). An
 * earlier version divided by concurrent+1, so a single turn only saw half the
 * budget and a tool-heavy answer died with `subrequest budget spent (essential)`
 * even though nothing else was running.
 */
export function runBudgetSlice(remaining: number, concurrentTurns: number): number {
  const sharers = Math.max(1, Math.floor(Number(concurrentTurns) || 1))
  const share = Math.floor(Math.max(0, remaining) / sharers)
  return Math.min(400, Math.max(12, share))
}

export class SubrequestBudget {
  /** The platform limit this budget mirrors. */
  readonly limit: number
  /** What the runtime is willing to spend: the limit minus redirect headroom. */
  private readonly usable: number
  private used = 0
  private redirects = 0
  private capHit = false
  private readonly reservations = new Map<string, RunTerminalReservation>()

  constructor(limit: number = DEFAULT_SUBREQUEST_BUDGET) {
    const parsed = Math.floor(Number(limit))
    this.limit = Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_SUBREQUEST_BUDGET
    this.usable = Math.max(1, this.limit - REDIRECT_HEADROOM)
  }

  /** Fetches charged to this invocation. */
  get spent(): number {
    return this.used
  }

  /** True once the platform itself rejected a fetch for this reason. */
  get exhausted(): boolean {
    return this.capHit
  }

  /** Redirect hops the platform already counted (each is one more subrequest). */
  get redirectsSeen(): number {
    return this.redirects
  }

  /** Terminal slots held by runs that are still in flight. */
  get reserved(): number {
    let total = 0
    for (const reservation of this.reservations.values()) total += reservation.held
    return total
  }

  remaining(): number {
    return Math.max(0, this.usable - this.used - this.redirects)
  }

  allows(tier: SubrequestTier): boolean {
    if (this.capHit) return false
    // Essential work may spend up to (not into) the runs' terminal reservations;
    // best-effort work must keep the essential headroom intact as well.
    return tier === 'essential'
      ? this.remaining() > this.reserved
      : this.remaining() > this.reserved + ESSENTIAL_HEADROOM
  }

  /** Reserves one subrequest when the tier still has headroom. */
  take(tier: SubrequestTier): boolean {
    if (!this.allows(tier)) return false
    this.used++
    return true
  }

  /**
   * Reserves the terminal headroom of one claimed run. Returns null when this
   * invocation cannot pay for the run's report (or already reserved it), so the
   * caller leaves the turn queued instead of claiming work it cannot finish.
   */
  reserveRun(runId: string): RunTerminalReservation | null {
    if (this.capHit) return null
    if (this.reservations.has(runId)) return null
    // One spare essential slot beyond the terminal slots, so the claim after the
    // reservation can actually be paid for instead of failing immediately.
    if (this.remaining() - this.reserved <= TERMINAL_RESERVE_PER_RUN) return null
    const reservation = new RunTerminalReservation(this, runId)
    this.reservations.set(runId, reservation)
    return reservation
  }

  /** Internal to RunTerminalReservation: charge one already-reserved slot. */
  chargeReserved(): boolean {
    if (this.capHit) return false
    this.used++
    return true
  }

  /** Internal to RunTerminalReservation.release(). */
  forgetRun(runId: string): void {
    this.reservations.delete(runId)
  }

  /**
   * The platform said this invocation is out of subrequests. No further fetch
   * can succeed, so stop spending (and stop retrying) until the next one.
   */
  markCapHit(): void {
    this.capHit = true
  }

  /** A response followed at least one redirect the platform already counted. */
  noteRedirect(): void {
    this.redirects++
  }

  /**
   * Best-effort subrequests one turn may spend. Dividing by the concurrent turn
   * slots keeps two chat turns and one background turn from consuming each
   * other's streaming budget while still leaving every run's terminal
   * reservation and the essential headroom.
   */
  bestEffortAllowance(slots: number): number {
    const divisor = Math.max(1, Math.floor(slots))
    const shareable = this.usable - TERMINAL_RESERVE_PER_RUN * divisor - ESSENTIAL_HEADROOM
    return Math.max(4, Math.floor(shareable / divisor))
  }
}

/** A fresh budget for one invocation. Unset/unusable values use the Free floor. */
export function createSubrequestBudget(configured?: string | number): SubrequestBudget {
  if (configured === undefined || configured === null || configured === '') {
    return new SubrequestBudget(DEFAULT_SUBREQUEST_BUDGET)
  }
  const parsed = Math.floor(Number(configured))
  return new SubrequestBudget(Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_SUBREQUEST_BUDGET)
}

/**
 * Counted `fetch()`: the only way runtime code talks to the backend, a model
 * provider, or an MCP server. Refuses to spend a subrequest the invocation does
 * not have, and records the platform cap so nothing else wastes an attempt.
 */
export async function fetchWithBudget(
  budget: SubrequestBudget,
  input: RequestInfo | URL,
  init: RequestInit | undefined,
  charge: SubrequestCharge,
): Promise<Response> {
  const label = typeof charge === 'string' ? charge : 'terminal'
  const allowed = typeof charge === 'string' ? budget.take(charge) : charge()
  if (!allowed) {
    throw new SubrequestBudgetExhaustedError(`subrequest budget spent (${label})`)
  }
  let response: Response
  try {
    response = await fetch(input, init)
  } catch (error) {
    if (isSubrequestLimitError(error)) budget.markCapHit()
    throw error
  }
  // A redirect chain costs one platform subrequest per hop. The response only
  // reveals that at least one hop happened, so count it before deciding again.
  if (response.redirected) budget.noteRedirect()
  return response
}

export interface EventBatchState {
  /** Events waiting in the buffer. */
  events: number
  /** Estimated buffered payload bytes. */
  bytes: number
  /** Milliseconds since the last successful flush. */
  sinceFlushMs: number
  /** Best-effort subrequests this turn already spent. */
  spent: number
  /** Best-effort subrequests this turn may spend. */
  allowance: number
  /** Whether the first delta already bypassed this gate. */
  firstFlushed: boolean
}

/**
 * Coalesces ordinary token deltas: the first one is flushed immediately (the
 * dashboard shows text at once), later ones are batched by bounded size or time
 * instead of one request per few tokens.
 */
export function shouldFlushEventBatch(state: EventBatchState): boolean {
  if (state.spent >= state.allowance) return false
  if (!state.firstFlushed && state.events > 0) return true
  return state.events >= EVENT_FLUSH_EVENTS ||
    state.bytes >= EVENT_FLUSH_BYTES ||
    state.sinceFlushMs >= EVENT_FLUSH_INTERVAL_MS
}
