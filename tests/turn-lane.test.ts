import assert from 'node:assert/strict'
import test from 'node:test'
import {
  BACKGROUND_LANE,
  INTERACTIVE_LANE,
  classifyRunReport,
  normalizeLane,
  planLaneBatches,
  shouldCompleteWake,
} from '../src/runtime/turn.ts'

test('the backend-assigned lane decides which capacity pool a turn uses', () => {
  assert.equal(normalizeLane('background'), BACKGROUND_LANE)
  assert.equal(normalizeLane(' BACKGROUND '), BACKGROUND_LANE)
  assert.equal(normalizeLane('interactive'), INTERACTIVE_LANE)
})

test('a missing lane stays interactive so older backends keep their capacity', () => {
  // Backends before the lane contract never send the field. Defaulting them to
  // the chat pool preserves today's behaviour instead of throttling chat to the
  // single background slot.
  assert.equal(normalizeLane(undefined), INTERACTIVE_LANE)
  assert.equal(normalizeLane(null), INTERACTIVE_LANE)
  assert.equal(normalizeLane(''), INTERACTIVE_LANE)
})

// Lane dispatch regression group.
//
// The drain used to `await` one lane before starting the next, so a batch of
// background turns held every chat turn in the same poll. These cases pin the
// planning rules that make concurrent dispatch safe: lanes are planned
// independently and the per-invocation limit is reserved before any turn runs.

test('a full background lane never hides the chat lane from the same poll', () => {
  const byLane = new Map<string, string[]>([
    [BACKGROUND_LANE, ['bg-1']],
    [INTERACTIVE_LANE, ['chat-1', 'chat-2']],
  ])
  const plan = planLaneBatches(
    byLane,
    (lane) => (lane === BACKGROUND_LANE ? 1 : 0),
    (lane) => (lane === BACKGROUND_LANE ? 1 : 2),
    0,
    10,
  )
  assert.deepEqual(plan.skipped, [BACKGROUND_LANE])
  assert.deepEqual(plan.batches, [{ lane: INTERACTIVE_LANE, items: ['chat-1', 'chat-2'] }])
  assert.equal(plan.reserved, 2)
})

test('each lane is capped by its own pool, not by the isolate total', () => {
  const byLane = new Map<string, string[]>([
    [INTERACTIVE_LANE, ['chat-1', 'chat-2']],
    [BACKGROUND_LANE, ['bg-1', 'bg-2']],
  ])
  const plan = planLaneBatches(
    byLane,
    (lane) => (lane === INTERACTIVE_LANE ? 1 : 0),
    (lane) => (lane === INTERACTIVE_LANE ? 2 : 1),
    0,
    10,
  )
  assert.deepEqual(plan.batches, [
    { lane: INTERACTIVE_LANE, items: ['chat-1'] },
    { lane: BACKGROUND_LANE, items: ['bg-1'] },
  ])
  assert.equal(plan.reserved, 2)
})

test('the per-invocation turn limit is reserved before any lane starts', () => {
  const byLane = new Map<string, string[]>([
    [INTERACTIVE_LANE, ['chat-1', 'chat-2']],
    [BACKGROUND_LANE, ['bg-1']],
  ])
  const plan = planLaneBatches(byLane, () => 0, () => 2, 9, 10)
  // Only one turn is left in this invocation; the first planned lane takes it.
  assert.deepEqual(plan.batches, [{ lane: INTERACTIVE_LANE, items: ['chat-1'] }])
  assert.deepEqual(plan.skipped, [BACKGROUND_LANE])
  assert.equal(plan.reserved, 1)
})

test('a spent invocation budget plans nothing instead of re-polling the batch', () => {
  const plan = planLaneBatches(new Map([[INTERACTIVE_LANE, ['chat-1']]]), () => 0, () => 2, 10, 10)
  assert.deepEqual(plan.batches, [])
  assert.deepEqual(plan.skipped, [INTERACTIVE_LANE])
  assert.equal(plan.reserved, 0)
})

// Terminal report regression group: an acknowledged or ambiguous finish must be
// retried as the idempotent PATCH only, never by re-running paid work.

test('the terminal report is idempotent and an ambiguous result is retried', () => {
  assert.equal(classifyRunReport(200), 'delivered')
  assert.equal(classifyRunReport(204), 'delivered')
  // The result is unknown, so the PATCH (not the turn) is retried.
  assert.equal(classifyRunReport(408), 'ambiguous')
  assert.equal(classifyRunReport(429), 'ambiguous')
  assert.equal(classifyRunReport(500), 'ambiguous')
  assert.equal(classifyRunReport(503), 'ambiguous')
  // A definitive rejection is not an acknowledgement and must not be retried.
  assert.equal(classifyRunReport(400), 'rejected')
  assert.equal(classifyRunReport(404), 'rejected')
  assert.equal(classifyRunReport(422), 'rejected')
})

test('a 409 report is a nonterminal conflict, never an acknowledgement', () => {
  // Backend contract (services/data_agent_service.go FinishRun): an already
  // terminal run replays 200 with the stored row, while 409
  // (`ErrDataAgentConflict`) is returned for a run that is still `queued` or for
  // an agent that moved to the legacy executor. In both the report was NOT
  // applied, so treating 409 as delivered lost the paid result and closed the DO
  // wake event on a run that never became terminal.
  assert.equal(classifyRunReport(409), 'rejected')
  assert.equal(shouldCompleteWake(classifyRunReport(409)), false)
})

test('the wake event is completed only on an acknowledged report', () => {
  assert.equal(shouldCompleteWake('delivered'), true)
  assert.equal(shouldCompleteWake('ambiguous'), false)
  assert.equal(shouldCompleteWake('rejected'), false)
  assert.equal(shouldCompleteWake('unsent'), false)
})
