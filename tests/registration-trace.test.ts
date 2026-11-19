import assert from 'node:assert/strict'
import test from 'node:test'
import { runtimeTrace, traceRegistration } from '../src/runtime/diagnostics.ts'
import { createSubrequestBudget } from '../src/runtime/subrequest-budget.ts'

test('runtime trace strips URL credentials/query/fragment and logs token presence only', () => {
  const trace = runtimeTrace({
    URL_BACKEND: 'https://name:private-password@example.com/api?key=private-query#private-fragment',
    WORKER_TOKEN: 'fng_aow_private-token', URL_BACKEND_DEV: 'https://ignored.example',
  }, 'test', 'scheduled')
  const serialized = JSON.stringify(trace)
  assert.equal(trace.backend_origin, 'https://example.com')
  assert.equal(trace.backend_base_path, '/api')
  assert.equal(trace.backend_valid, false)
  assert.equal(trace.dev_override_ignored, true)
  assert.equal(trace.token_configured, true)
  for (const secret of ['private-password', 'private-query', 'private-fragment', 'fng_aow_private-token']) {
    assert.equal(serialized.includes(secret), false)
  }
})

test('registration failure retains status and correlation without raw body details', async () => {
  const originalFetch = globalThis.fetch
  const originalLog = console.log
  const originalError = console.error
  const logs: Record<string, unknown>[] = []
  const trace = runtimeTrace({ URL_BACKEND: 'https://example.com', WORKER_TOKEN: 'fng_aow_private-token' }, 'test', 'scheduled')
  let requestHeaders: Headers | undefined
  globalThis.fetch = async (_url, init) => {
    requestHeaders = new Headers(init?.headers)
    return new Response(JSON.stringify({ error: 'INVALID_TOKEN', message: 'private-body-secret' }), {
      status: 401, headers: { 'content-type': 'application/json', 'x-request-id': 'f235ab3a-fd34-40e0-b024-093657821d31' },
    })
  }
  console.log = (entry) => { logs.push(entry) }
  console.error = (entry) => { logs.push(entry) }
  try {
    await traceRegistration(trace, 'https://example.com', 'fng_aow_private-token', 'heartbeat', { version: 'test' }, createSubrequestBudget())
    const response = logs.find((entry) => entry.event === 'runtime.heartbeat.response')
    assert.equal(response?.status, 401)
    assert.equal(response?.error_code, 'INVALID_TOKEN')
    assert.equal(response?.trace_id, requestHeaders?.get('x-trace-id'))
    assert.equal(response?.backend_request_id, 'f235ab3a-fd34-40e0-b024-093657821d31')
    assert.equal(JSON.stringify(logs).includes('private-body-secret'), false)
    assert.equal(JSON.stringify(logs).includes('fng_aow_private-token'), false)
  } finally {
    globalThis.fetch = originalFetch
    console.log = originalLog
    console.error = originalError
  }
})
