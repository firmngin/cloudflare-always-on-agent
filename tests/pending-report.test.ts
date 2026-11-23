import assert from 'node:assert/strict'
import test from 'node:test'
import {
  MAX_PENDING_CONTENT_BYTES,
  MAX_PENDING_REPORTS,
  PENDING_REPORT_TTL_MS,
  clearPendingReportState,
  pendingReportFor,
  recordPendingReportState,
  toTerminalReport,
  type PendingReportState,
  type PendingRunReport,
} from '../src/runtime/pending-report.ts'
import { shouldCompleteWake } from '../src/runtime/turn.ts'

/** node:assert does not narrow types here (tests are outside the tsconfig project). */
function requirePending(report: PendingRunReport | null): PendingRunReport {
  if (!report) throw new Error('expected a pending report')
  return report
}

// Wake-unconfirmed regression group: a turn that finished but whose terminal
// report was never acknowledged must be replayed, not re-executed. The backend
// resumes a claim for the same token, so a retried wake would re-enter the model
// turn (and re-spend the user's key) if the report were not fenced here.

test('an unacknowledged report is kept and replayed instead of re-running the turn', () => {
  const now = 1_795_000_000_000
  const recorded = recordPendingReportState(undefined, 'run-1', {
    status: 'succeeded',
    content: 'the paid answer',
    output: 'the paid answer',
  }, now)

  const pending = requirePending(pendingReportFor(recorded, 'run-1', now))
  // `recorded_at` is object bookkeeping; the PATCH body is strictly decoded by
  // the backend, so only the contracted fields are sent back.
  assert.deepEqual(JSON.parse(JSON.stringify(toTerminalReport(pending))), {
    status: 'succeeded',
    content: 'the paid answer',
    tool_loop_hit: false,
  })
  assert.equal(pendingReportFor(recorded, 'another-run', now), null)
})

test('a failed report keeps its code and message for the replay', () => {
  const now = 1_795_000_000_000
  const recorded = recordPendingReportState(undefined, 'run-2', {
    status: 'failed',
    error_code: 'WORKER_ERROR',
    error_message: 'turn failed',
  }, now)

  const pending = requirePending(pendingReportFor(recorded, 'run-2', now))
  assert.deepEqual(JSON.parse(JSON.stringify(toTerminalReport(pending))), {
    status: 'failed',
    error_code: 'WORKER_ERROR',
    error_message: 'turn failed',
    tool_loop_hit: false,
  })
})

test('a replayed report keeps the step-guard flag it was recorded with', () => {
  const now = 1_795_000_000_000
  const recorded = recordPendingReportState(undefined, 'run-guarded', {
    status: 'succeeded',
    content: 'stopped by MAX_STEPS',
    tool_loop_hit: true,
  }, now)

  const pending = requirePending(pendingReportFor(recorded, 'run-guarded', now))
  // Replay sends the stored value, never a recomputed one: the turn is gone.
  const replayed = JSON.parse(JSON.stringify(toTerminalReport(pending)))
  assert.deepEqual(replayed, { status: 'succeeded', content: 'stopped by MAX_STEPS', tool_loop_hit: true })
  assert.equal(typeof replayed.tool_loop_hit, 'boolean')
})

test('a fenced report is dropped once the backend reaper owns the run', () => {
  const now = 1_795_000_000_000
  const recorded = recordPendingReportState(undefined, 'run-3', { status: 'succeeded', content: 'x' }, now)
  assert.equal(pendingReportFor(recorded, 'run-3', now + PENDING_REPORT_TTL_MS), recorded.reports['run-3'])
  assert.equal(pendingReportFor(recorded, 'run-3', now + PENDING_REPORT_TTL_MS + 1), null)
  // Expired entries are pruned on the next write so the object value stays small.
  const after = recordPendingReportState(recorded, 'run-4', { status: 'succeeded' }, now + PENDING_REPORT_TTL_MS + 1)
  assert.deepEqual(Object.keys(after.reports), ['run-4'])
})

test('a delivered replay clears the fence', () => {
  const now = 1_795_000_000_000
  const recorded = recordPendingReportState(undefined, 'run-5', { status: 'succeeded', content: 'x' }, now)
  const cleared = clearPendingReportState(recorded, 'run-5', now + 1_000)
  assert.equal(pendingReportFor(cleared, 'run-5', now + 1_000), null)
})

test('an oversized answer keeps only its status so the value stays storable', () => {
  const now = 1_795_000_000_000
  const huge = 'a'.repeat(MAX_PENDING_CONTENT_BYTES + 1)
  const recorded = recordPendingReportState(undefined, 'run-6', { status: 'succeeded', content: huge }, now)
  const pending = requirePending(pendingReportFor(recorded, 'run-6', now))
  // The backend falls back to the streamed final event when no content arrives.
  assert.deepEqual(JSON.parse(JSON.stringify(toTerminalReport(pending))), { status: 'succeeded', tool_loop_hit: false })
})

test('pending fences are bounded to the newest runs', () => {
  const now = 1_795_000_000_000
  let state: PendingReportState | undefined
  for (let index = 0; index < MAX_PENDING_REPORTS + 4; index++) {
    state = recordPendingReportState(state, `run-${index}`, { status: 'succeeded', content: `${index}` }, now + index)
  }
  const bounded = state ?? { reports: {} }
  assert.equal(Object.keys(bounded.reports).length, MAX_PENDING_REPORTS)
  const newest = now + MAX_PENDING_REPORTS + 3
  assert.ok(pendingReportFor(bounded, `run-${MAX_PENDING_REPORTS + 3}`, newest))
  assert.equal(pendingReportFor(bounded, 'run-0', newest), null)
})

test('the wake is only completed when the report was acknowledged', () => {
  // The wake marker and the DO completion both hang off this single rule.
  assert.equal(shouldCompleteWake('delivered'), true)
  assert.equal(shouldCompleteWake('unsent'), false)
})
