export interface McpClaim {
  mcp_enabled?: boolean
}

/**
 * Execution lanes mirror the backend's assignment. A Worker never chooses its
 * own lane: the value arrives signed in the wake payload, in the inbox item, or
 * in the claim response.
 */
export const INTERACTIVE_LANE = 'interactive'
export const BACKGROUND_LANE = 'background'

/** Unknown or missing lanes are interactive, the safe (user-facing) default. */
export function normalizeLane(lane?: string | null): string {
  return String(lane || '').trim().toLowerCase() === BACKGROUND_LANE ? BACKGROUND_LANE : INTERACTIVE_LANE
}

export function shouldConnectMcp(claim: McpClaim): boolean {
  // `undefined` is an old backend: preserve historical MCP discovery until
  // that backend starts sending the authoritative boolean.
  return claim.mcp_enabled !== false
}

export function shouldFlushFirstDelta(firstDeltaFlushed: boolean): boolean {
  return !firstDeltaFlushed
}

/** How the runtime reads a terminal run-report (PATCH) result. */
export type RunReportOutcome = 'delivered' | 'ambiguous' | 'rejected' | 'unsent'

/** The terminal payload a run sends once its paid work is finished. */
export interface TerminalRunReport {
  status: 'succeeded' | 'failed'
  content?: string
  output?: string
  error_code?: string
  error_message?: string
  /** True only when MAX_STEPS ended the turn; false on any other outcome. */
  tool_loop_hit?: boolean
}

/**
 * What an engine turn returns: its final content and whether the step guard
 * (`MAX_STEPS`) ended it while it still wanted another assistant turn.
 */
export interface TurnOutput {
  content: string
  toolLoopHit: boolean
}

/**
 * Only 2xx acknowledges a report. The backend answers 2xx for the idempotent
 * replay of an already-final run (`FinishRun` re-reads the terminal row and
 * returns it unchanged), so a replayed report is an ack too. 409 is NOT: it is
 * returned when the run is still `queued` (no claim owns it) or the agent moved
 * to the legacy executor, i.e. the report was not applied. 408/429/5xx leave
 * the result unknown, and any other 4xx is a definitive rejection. Retrying
 * never re-runs the paid model/tool work, only the idempotent report.
 */
export function classifyRunReport(status: number): RunReportOutcome {
  if (status >= 200 && status < 300) return 'delivered'
  if (status === 408 || status === 429 || status >= 500) return 'ambiguous'
  return 'rejected'
}

/**
 * The wake event may only be completed on an acknowledged report. A finished
 * turn whose report was ambiguous/unsent/rejected is kept as a pending report
 * (./pending-report.ts) and replayed by the next wake, so the event must stay
 * open for the backend to retry instead of looking committed.
 */
export function shouldCompleteWake(outcome: RunReportOutcome): boolean {
  return outcome === 'delivered'
}

export function turnBatchCapacity(
  active: number,
  handled: number,
  maxConcurrent: number,
  maxPerInvocation: number,
): number {
  return Math.max(0, Math.min(
    maxConcurrent - active,
    maxPerInvocation - handled,
  ))
}

/**
 * Interleaves items so that consecutive entries come from different agents.
 *
 * Under load one busy agent can queue several turns while another has one. Slicing
 * the poll in arrival order would let the busy agent take every slot, so the
 * second agent's single turn waits a whole invocation. Round-robin keeps the
 * lanes busy across agents instead of across one agent's backlog.
 */
export function interleaveByAgent<T>(items: T[], keyOf: (item: T) => string): T[] {
  if (items.length < 2) return items.slice()
  const groups = new Map<string, T[]>()
  const order: string[] = []
  for (const item of items) {
    const key = keyOf(item) || ''
    const group = groups.get(key)
    if (group) {
      group.push(item)
      continue
    }
    groups.set(key, [item])
    order.push(key)
  }
  if (order.length < 2) return items.slice()
  const out: T[] = []
  for (let index = 0; out.length < items.length; index += 1) {
    for (const key of order) {
      const group = groups.get(key)
      if (group && group[index] !== undefined) out.push(group[index])
    }
  }
  return out
}

export type LaneBatch<T> = { lane: string; items: T[] }

export type LaneBatchPlan<T> = {
  batches: Array<LaneBatch<T>>
  /** Turns this poll may start; reserved before any of them runs. */
  reserved: number
  /** Lanes left untouched because they had no capacity. */
  skipped: string[]
}

/**
 * Plans one inbox poll across execution lanes.
 *
 * Every lane is planned before anything starts, so the per-invocation limit is
 * reserved up front instead of being incremented by whichever turn happens to
 * finish first. A lane at its own cap is skipped without spending another
 * lane's budget, and the caller dispatches all returned batches concurrently:
 * awaiting one lane before starting the next let a full background lane hold
 * every chat turn in the same poll.
 */
export function planLaneBatches<T>(
  byLane: Map<string, T[]> | Iterable<[string, T[]]>,
  activeTurnsFor: (lane: string) => number,
  capFor: (lane: string) => number,
  handled: number,
  maxPerInvocation: number,
  /** Identity used for fairness (agent id). Omitted = FIFO, the old behaviour. */
  keyOf?: (item: T) => string,
): LaneBatchPlan<T> {
  const batches: Array<LaneBatch<T>> = []
  const skipped: string[] = []
  let reserved = 0
  for (const [lane, laneItems] of byLane) {
    if (!laneItems || !laneItems.length) continue
    const capacity = turnBatchCapacity(activeTurnsFor(lane), handled + reserved, capFor(lane), maxPerInvocation)
    if (capacity <= 0) {
      skipped.push(lane)
      continue
    }
    const ordered = keyOf ? interleaveByAgent(laneItems, keyOf) : laneItems
    const items = ordered.slice(0, capacity)
    batches.push({ lane, items })
    reserved += items.length
  }
  return { batches, reserved, skipped }
}
