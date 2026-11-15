import assert from 'node:assert/strict'
import test from 'node:test'
import { inboxPollBudget, withRequestTimeout } from '../src/runtime/endpoints.ts'

test('inbox wait and transport timeout fit the remaining invocation budget', () => {
  assert.deepEqual(inboxPollBudget(50_000), { waitSeconds: 25, timeoutMs: 40_000 })
  assert.deepEqual(inboxPollBudget(9_500), { waitSeconds: 8, timeoutMs: 9_500 })
  assert.equal(inboxPollBudget(1_999), null)
})

test('timeout aborts the request while its response body is still pending', async () => {
  let requestSignal: AbortSignal | undefined
  await assert.rejects(withRequestTimeout(async (signal) => {
    requestSignal = signal
    // Model fetch headers already received, with body consumption still pending.
    return await new Promise<never>((_resolve, reject) => {
      signal.addEventListener('abort', () => reject(signal.reason), { once: true })
    })
  }, 5, 'inbox poll timed out'), /inbox poll timed out/)
  assert.equal(requestSignal?.aborted, true)
})
