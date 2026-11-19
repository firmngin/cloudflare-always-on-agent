import assert from 'node:assert/strict'
import test from 'node:test'
import { callModel } from '../src/pi/stream-fn.ts'
import { shouldConnectMcp, shouldFlushFirstDelta, turnBatchCapacity } from '../src/runtime/turn.ts'

test('MCP preflight is skipped only for an authoritative false claim', () => {
  assert.equal(shouldConnectMcp({ mcp_enabled: false }), false)
  assert.equal(shouldConnectMcp({ mcp_enabled: true }), true)
  assert.equal(shouldConnectMcp({}), true)
})

test('only the first text delta bypasses the normal batch threshold', () => {
  assert.equal(shouldFlushFirstDelta(false), true)
  assert.equal(shouldFlushFirstDelta(true), false)
})

test('turn batch capacity never exceeds two active turns or the invocation remainder', () => {
  assert.equal(turnBatchCapacity(0, 0, 2, 10), 2)
  assert.equal(turnBatchCapacity(1, 0, 2, 10), 1)
  assert.equal(turnBatchCapacity(0, 9, 2, 10), 1)
  assert.equal(turnBatchCapacity(2, 0, 2, 10), 0)
  assert.equal(turnBatchCapacity(0, 10, 2, 10), 0)
})

test('model provider request receives and observes the turn abort signal', async () => {
  const originalFetch = globalThis.fetch
  let providerSignal: AbortSignal | null | undefined
  globalThis.fetch = async (_input, init) => {
    providerSignal = init?.signal
    return await new Promise<Response>((_resolve, reject) => {
      init?.signal?.addEventListener('abort', () => reject(init.signal?.reason), { once: true })
    })
  }

  try {
    const controller = new AbortController()
    const pending = callModel(
      'test-model',
      [{ role: 'user', content: 'hello' }],
      [],
      async () => {},
      'https://provider.invalid/v1',
      'openai',
      controller.signal,
      'run-abort',
    )
    controller.abort(new Error('turn timed out'))

    await assert.rejects(pending, /turn timed out/)
    assert.equal(providerSignal?.aborted, true)
  } finally {
    globalThis.fetch = originalFetch
  }
})
