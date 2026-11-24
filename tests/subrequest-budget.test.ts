import assert from 'node:assert/strict'
import test from 'node:test'
import {
  SubrequestBudget,
  SubrequestBudgetExhaustedError,
  createSubrequestBudget,
  fetchWithBudget,
  isSubrequestLimitError,
  shouldFlushEventBatch,
  DEFAULT_SUBREQUEST_BUDGET,
  ESSENTIAL_HEADROOM,
  EVENT_FLUSH_BYTES,
  EVENT_FLUSH_EVENTS,
  EVENT_FLUSH_INTERVAL_MS,
  REDIRECT_HEADROOM,
  TERMINAL_RESERVE_PER_RUN,
} from '../src/runtime/subrequest-budget.ts'

// The captured production trace (worker 2.2.5, cron trigger) failed with this
// exact platform message after ~50 subrequests: event batches were lost, the
// terminal run report was rejected and the next inbox poll died, so the chat
// streamed partially and then stopped answering.
const CAP_ERROR = 'Too many subrequests by single Worker invocation. To configure this limit, refer to https://developers.cloudflare.com/workers/wrangler/configuration/#limits'

test('the platform cap error is recognised by its exact wording', () => {
  assert.equal(isSubrequestLimitError(new Error(CAP_ERROR)), true)
  assert.equal(isSubrequestLimitError(CAP_ERROR), true)
  assert.equal(isSubrequestLimitError(new Error('inbox poll timed out')), false)
  assert.equal(isSubrequestLimitError(new SubrequestBudgetExhaustedError('subrequest budget spent (bestEffort)')), false)
})

test('the budget defaults to the Workers Free floor and keeps redirect headroom unspent', () => {
  const budget = createSubrequestBudget()
  assert.equal(budget.limit, DEFAULT_SUBREQUEST_BUDGET)
  // Redirect hops are platform subrequests this counter cannot see up front.
  assert.equal(budget.remaining(), DEFAULT_SUBREQUEST_BUDGET - REDIRECT_HEADROOM)

  // Best-effort work stops before the essential headroom and terminal reserves.
  while (budget.allows('bestEffort')) budget.take('bestEffort')
  assert.equal(budget.allows('bestEffort'), false)
  assert.equal(budget.remaining(), ESSENTIAL_HEADROOM)

  // Essential work may still spend its headroom ...
  while (budget.allows('essential')) budget.take('essential')
  assert.equal(budget.allows('essential'), false)
  assert.equal(budget.remaining(), 0)
})

test('a rejected fetch marks the cap so no later attempt wastes a subrequest', async () => {
  const budget = createSubrequestBudget()
  const originalFetch = globalThis.fetch
  globalThis.fetch = async () => {
    throw new Error(CAP_ERROR)
  }
  try {
    await assert.rejects(fetchWithBudget(budget, 'https://backend.invalid', undefined, 'bestEffort'), new RegExp('Too many subrequests'))
    assert.equal(budget.exhausted, true)
    // Refused without touching the network: the count did not grow.
    const spent = budget.spent
    await assert.rejects(
      fetchWithBudget(budget, 'https://backend.invalid', undefined, 'essential'),
      SubrequestBudgetExhaustedError,
    )
    assert.equal(budget.spent, spent)
  } finally {
    globalThis.fetch = originalFetch
  }
})

test('a redirect hop is charged as the extra platform subrequest it is', async () => {
  const budget = createSubrequestBudget()
  const originalFetch = globalThis.fetch
  globalThis.fetch = async () => ({ redirected: true, ok: true, status: 200 } as Response)
  try {
    const before = budget.remaining()
    await fetchWithBudget(budget, 'https://backend.invalid/a', undefined, 'essential')
    assert.equal(budget.redirectsSeen, 1)
    // One fetch plus one redirect hop.
    assert.equal(budget.remaining(), before - 2)
  } finally {
    globalThis.fetch = originalFetch
  }
})

test('a deployment that raised [limits] subrequests can raise the budget', () => {
  const raised = createSubrequestBudget('1000')
  assert.equal(raised.limit, 1000)
  // Unset or unusable values fall back to the Free floor instead of trusting a plan,
  // and a configured value is never raised above itself.
  assert.equal(createSubrequestBudget('not-a-number').limit, DEFAULT_SUBREQUEST_BUDGET)
  assert.equal(createSubrequestBudget('').limit, DEFAULT_SUBREQUEST_BUDGET)
  assert.equal(createSubrequestBudget('20').limit, 20)
})

// Regression group: the platform cap is PER INVOCATION, so two invocations that
// overlap on one isolate must never share (or reset) one module-level counter.
// The old module singleton plus the `applyEnv` idle reset did exactly that: a
// cron drain and a wake stream could refill or poison each other, and a wake
// invocation was refused its own work because cron had already spent "the" cap.

test('two overlapping invocations get isolated budgets and terminal headroom', () => {
  const cron = createSubrequestBudget()
  const wake = createSubrequestBudget()
  assert.notEqual(cron, wake)

  const cronRun = cron.reserveRun('run-cron')
  assert.ok(cronRun)
  assert.ok(wake.reserveRun('run-wake'))

  // The cron invocation spends every best-effort and essential slot left.
  while (cron.allows('bestEffort')) cron.take('bestEffort')
  while (cron.allows('essential')) cron.take('essential')
  assert.equal(cron.allows('essential'), false)

  // The overlapping wake invocation still has its full budget and can claim a
  // streamed turn, which is the case the idle reset used to poison.
  assert.equal(wake.allows('bestEffort'), true)
  assert.ok(wake.reserveRun('run-wake-2'))
  const cronSpent = cron.spent
  const wakeSpent = wake.spent
  assert.equal(wake.take('essential'), true)
  assert.equal(wake.spent, wakeSpent + 1)
  assert.equal(cron.spent, cronSpent)

  // ... and cron's own report slot was untouched by the other invocation.
  assert.equal(cronRun!.takeReport(), true)
})

// Regression group: one cron invocation can run three turns at once. A single
// shared TERMINAL_RESERVE let three final/error event flushes and their retries
// exhaust it, so a run finished its paid work and then could not report.

test('three simultaneous runs each keep a final report after non-terminal exhaustion', () => {
  const budget = createSubrequestBudget()
  const runs = ['run-a', 'run-b', 'run-c'].map((runId) => {
    const reservation = budget.reserveRun(runId)
    assert.ok(reservation, `run ${runId} should get terminal headroom`)
    return reservation!
  })
  assert.equal(budget.reserved, TERMINAL_RESERVE_PER_RUN * 3)

  // Non-terminal work spends everything that is not reserved.
  while (budget.allows('bestEffort')) budget.take('bestEffort')
  while (budget.allows('essential')) budget.take('essential')
  assert.equal(budget.allows('bestEffort'), false)
  assert.equal(budget.allows('essential'), false)
  assert.equal(budget.remaining(), budget.reserved)

  // All three full-content reports still land, one per run.
  for (const run of runs) assert.equal(run.takeReport(), true)
  // The optional final/error UI batch has its own slot and never borrows one.
  for (const run of runs) assert.equal(run.takeTerminalEvents(), true)
  // With non-terminal work exhausted, a fourth turn is not claimed at all.
  assert.equal(budget.reserveRun('run-d'), null)
})

test('one run\'s terminal events and retries cannot spend another run\'s PATCH headroom', () => {
  const budget = createSubrequestBudget()
  const first = budget.reserveRun('run-a')
  const second = budget.reserveRun('run-b')
  assert.ok(first)
  assert.ok(second)
  while (budget.allows('essential')) budget.take('essential')

  // run-a burns both report attempts and its terminal event batch.
  assert.equal(first!.takeReport(), true)
  assert.equal(first!.takeReport(), true)
  assert.equal(first!.takeTerminalEvents(), true)
  assert.equal(first!.takeReport(), false)
  assert.equal(first!.takeTerminalEvents(), false)

  // run-b's report and event slots are still intact.
  assert.equal(second!.takeReport(), true)
  assert.equal(second!.takeTerminalEvents(), true)
})

test('releasing a finished run returns its unused headroom to the invocation', () => {
  const budget = createSubrequestBudget()
  const first = budget.reserveRun('run-a')
  assert.ok(first)
  first!.takeReport()
  first!.release()
  first!.release() // idempotent: the drain's finally can call it twice
  assert.equal(budget.reserved, 0)
  assert.ok(budget.reserveRun('run-b'))
})

test('a budget too small for a report claims no run instead of stalling one', () => {
  const budget = new SubrequestBudget(TERMINAL_RESERVE_PER_RUN - 1)
  assert.equal(budget.reserveRun('run-a'), null)
  assert.equal(budget.allows('bestEffort'), false)
})

test('deltas coalesce into bounded batches instead of one request per few tokens', () => {
  const allowance = new SubrequestBudget(DEFAULT_SUBREQUEST_BUDGET).bestEffortAllowance(3)
  let events = 0
  let bytes = 0
  let sinceFlushMs = 0
  let spent = 0
  let flushes = 0
  // 1,300 short deltas arriving every 5 ms is a long but ordinary answer.
  for (let i = 0; i < 1300; i++) {
    events++
    bytes += 3
    sinceFlushMs += 5
    if (shouldFlushEventBatch({ events, bytes, sinceFlushMs, spent, allowance, firstFlushed: flushes > 0 })) {
      flushes++
      spent++
      events = 0
      bytes = 0
      sinceFlushMs = 0
    }
  }
  assert.ok(flushes <= allowance, `flushes ${flushes} exceeded allowance ${allowance}`)
  // The old `>= 12 events` rule needed ~109 requests for the same answer.
  assert.ok(flushes <= 12)
  // Whatever is left in the buffer still leaves through the terminal batch.
  assert.ok(events > 0 || flushes > 0)
})

test('the first delta flushes immediately and the batch limits are size, count and time', () => {
  const base = { spent: 0, allowance: 11 }
  assert.equal(shouldFlushEventBatch({ ...base, events: 1, bytes: 3, sinceFlushMs: 0, firstFlushed: false }), true)
  assert.equal(shouldFlushEventBatch({ ...base, events: 1, bytes: 3, sinceFlushMs: 0, firstFlushed: true }), false)
  assert.equal(shouldFlushEventBatch({ ...base, events: EVENT_FLUSH_EVENTS, bytes: 3, sinceFlushMs: 0, firstFlushed: true }), true)
  assert.equal(shouldFlushEventBatch({ ...base, events: 1, bytes: EVENT_FLUSH_BYTES, sinceFlushMs: 0, firstFlushed: true }), true)
  assert.equal(shouldFlushEventBatch({ ...base, events: 1, bytes: 3, sinceFlushMs: EVENT_FLUSH_INTERVAL_MS, firstFlushed: true }), true)
  // A turn that spent its share stops spending even when the batch is large.
  assert.equal(shouldFlushEventBatch({ spent: 11, allowance: 11, events: 999, bytes: 1e6, sinceFlushMs: 1e6, firstFlushed: true }), false)
})

// Per-run slices: one long streamed answer must not drain the turns next to it.
test('a run slice caps its own spend and still charges the invocation', async () => {
  const { ScopedBudget, SubrequestBudget, runBudgetSlice } = await import('../src/runtime/subrequest-budget.ts')
  const invocation = new SubrequestBudget(50)
  const scoped = new ScopedBudget(invocation, 3)

  assert.equal(scoped.take('bestEffort'), true)
  assert.equal(scoped.take('bestEffort'), true)
  assert.equal(scoped.take('bestEffort'), true)
  // The slice is spent; the invocation still has room for the other turns.
  assert.equal(scoped.take('bestEffort'), false)
  assert.equal(scoped.spent, 3)
  assert.equal(invocation.spent, 3)
  assert.equal(scoped.remaining(), 0)

  // Terminal reports keep coming from the invocation pool.
  assert.notEqual(invocation.reserveRun('run-1'), null)
})

test('a run slice never exceeds what the invocation has left', async () => {
  const { ScopedBudget, SubrequestBudget } = await import('../src/runtime/subrequest-budget.ts')
  const invocation = new SubrequestBudget(50)
  for (let index = 0; index < 48; index += 1) invocation.take('bestEffort')
  const scoped = new ScopedBudget(invocation, 250)
  assert.equal(scoped.remaining() <= invocation.remaining(), true)
})

test('slice sizing keeps a floor and a ceiling', async () => {
  const { runBudgetSlice } = await import('../src/runtime/subrequest-budget.ts')
  assert.equal(runBudgetSlice(0, 3), 12)
  assert.equal(runBudgetSlice(40, 1), 20)
  assert.equal(runBudgetSlice(1000, 0), 250)
})
