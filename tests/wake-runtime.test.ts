import assert from 'node:assert/strict'
import test from 'node:test'
import { classifyRuntimeRoute, resolveWakeCallbackUrl } from '../src/runtime/wake-route.ts'
import { verifyWakeSignature } from '../src/runtime/wake-auth.ts'
import { authorizeWakeState, completeWakeState, releaseWakeState } from '../src/runtime/wake-security.ts'
import { readBoundedRequestBody, RequestBodyTooLargeError } from '../src/runtime/bounded-body.ts'
import { scheduleSyncOwnsAgent } from '../src/runtime/schedule-ownership.ts'

const deploymentId = '0df766cb-ad1f-4422-b6ba-937ba706b87c'
const workspaceId = '7492252c-72a3-4d68-befb-07da8484cc23'
const secret = 'wake-test-secret-that-is-at-least-32-bytes'

test('runtime route exposes only health and authenticated wake seams', () => {
  assert.equal(classifyRuntimeRoute('GET', '/'), 'health')
  assert.equal(classifyRuntimeRoute('POST', '/v1/wake'), 'wake')
  assert.equal(classifyRuntimeRoute('POST', '/v1/wake', '?unexpected=1'), 'not_found')
  assert.equal(classifyRuntimeRoute('GET', '/v1/wake'), 'method_not_allowed')
  assert.equal(classifyRuntimeRoute('POST', '/'), 'not_found')
  assert.equal(classifyRuntimeRoute('POST', '/anything-else'), 'not_found')
})

test('wake signature binds body path deployment and workspace', async () => {
  const now = 1_795_000_000
  const body = new TextEncoder().encode('{"event_id":"event"}')
  const request = await signedWakeRequest(body, now)
  const binding = await verifyWakeSignature(request, body, secret, now)

  assert.equal(binding.deploymentId, deploymentId)
  assert.equal(binding.workspaceId, workspaceId)

  const tamperedBody = new TextEncoder().encode('{"event_id":"tampered"}')
  await assert.rejects(verifyWakeSignature(request, tamperedBody, secret, now), /body hash mismatch/)

  const tamperedWorkspace = new Request(request)
  tamperedWorkspace.headers.set('X-Firmngin-Workspace-ID', 'dbe847c5-1369-4105-a7af-043122d76228')
  await assert.rejects(verifyWakeSignature(tamperedWorkspace, body, secret, now), /signature is invalid/)
})

test('wake signature rejects stale requests', async () => {
  const body = new TextEncoder().encode('{}')
  const request = await signedWakeRequest(body, 1_795_000_000)
  await assert.rejects(verifyWakeSignature(request, body, secret, 1_795_000_301), /outside the replay window/)
})

test('wake security rejects replay and duplicate events', () => {
  const first = authorizeWakeState(undefined, 'nonce-one-123456', 'event-1', 1_000)
  assert.equal(first.accepted, true)

  const replay = authorizeWakeState(first.state, 'nonce-one-123456', 'event-2', 2_000)
  assert.equal(replay.duplicate, true)

  const sameEvent = authorizeWakeState(first.state, 'nonce-two-123456', 'event-1', 2_000)
  assert.equal(sameEvent.duplicate, true)
  assert.equal(sameEvent.duplicateCompleted, false)

  const released = releaseWakeState(first.state, 'event-1')
  const retry = authorizeWakeState(released, 'nonce-three-123456', 'event-1', 3_000)
  assert.equal(retry.accepted, true)

  const completed = completeWakeState(retry.state, 'event-1', 4_000)
  const afterComplete = authorizeWakeState(completed, 'nonce-four-123456', 'event-1', 5_000)
  assert.equal(afterComplete.duplicate, true)
  assert.equal(afterComplete.duplicateCompleted, true)
})

test('wake security applies a durable per-agent request budget', () => {
  let state
  for (let index = 0; index < 30; index++) {
    const decision = authorizeWakeState(state, `nonce-${index.toString().padStart(16, '0')}`, `event-${index}`, 1_000 + index)
    assert.equal(decision.accepted, true)
    state = decision.state
  }
  const limited = authorizeWakeState(state, 'nonce-rate-limit-0001', 'event-over-limit', 2_000)
  assert.equal(limited.rateLimited, true)
})

test('wake request body is bounded while streaming', async () => {
  const exact = new Request('https://worker.example/v1/wake', {
    method: 'POST',
    body: new Uint8Array(8),
  })
  assert.equal((await readBoundedRequestBody(exact, 8)).byteLength, 8)

  const oversized = new Request('https://worker.example/v1/wake', {
    method: 'POST',
    body: new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array(5))
        controller.enqueue(new Uint8Array(4))
        controller.close()
      },
    }),
    duplex: 'half',
  } as RequestInit & { duplex: 'half' })
  await assert.rejects(readBoundedRequestBody(oversized, 8), RequestBodyTooLargeError)
})

test('successful empty schedule sync disables legacy scheduling', () => {
  assert.equal(scheduleSyncOwnsAgent([]), true)
  assert.equal(scheduleSyncOwnsAgent(null), false)
})

test('wake callback prefers WORKER_BASE_URL and fails closed when it is invalid', () => {
  // New provisioning: one origin, exact /v1/wake derived from it.
  assert.deepEqual(resolveWakeCallbackUrl({ baseUrl: 'https://worker.example.com' }), {
    url: 'https://worker.example.com/v1/wake', source: 'worker_base_url',
  })
  assert.deepEqual(resolveWakeCallbackUrl({ baseUrl: 'https://worker.example.com/' }), {
    url: 'https://worker.example.com/v1/wake', source: 'worker_base_url',
  })
  // Legacy fallback stays byte-for-byte the old rule.
  assert.deepEqual(resolveWakeCallbackUrl({ baseUrl: '   ', callbackUrl: 'https://old.example.com/v1/wake' }), {
    url: 'https://old.example.com/v1/wake', source: 'wake_callback_url',
  })
  // Both configured: the base wins, the stale callback is ignored.
  assert.deepEqual(
    resolveWakeCallbackUrl({ baseUrl: 'https://new.example.com', callbackUrl: 'https://old.example.com/v1/wake' }),
    { url: 'https://new.example.com/v1/wake', source: 'worker_base_url' },
  )
  for (const invalid of [
    'http://worker.example.com', 'https://worker.example.com/v1/wake', 'https://worker.example.com/a/..',
    'https://worker.example.com/%2e%2e', 'https://worker.example.com/x/?q=1', 'https://worker.example.com/#f',
    'https://user:pass@worker.example.com', 'not-a-url',
  ]) {
    assert.deepEqual(
      resolveWakeCallbackUrl({ baseUrl: invalid, callbackUrl: 'https://old.example.com/v1/wake' }),
      { url: null, source: null },
      invalid,
    )
  }
})

async function signedWakeRequest(body: Uint8Array, timestamp: number): Promise<Request> {
  const nonce = 'nonce-signature-123456'
  const bodyHash = hex(new Uint8Array(await crypto.subtle.digest('SHA-256', body)))
  const canonical = [
    'POST',
    '/v1/wake',
    String(timestamp),
    nonce,
    bodyHash,
    deploymentId,
    workspaceId,
  ].join('\n')
  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  )
  const signature = hex(new Uint8Array(await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(canonical))))
  return new Request('https://worker.example/v1/wake', {
    method: 'POST',
    body,
    headers: {
      'X-Firmngin-Timestamp': String(timestamp),
      'X-Firmngin-Nonce': nonce,
      'X-Firmngin-Deployment-ID': deploymentId,
      'X-Firmngin-Workspace-ID': workspaceId,
      'X-Firmngin-Body-SHA256': bodyHash,
      'X-Firmngin-Signature': `v1=${signature}`,
    },
  })
}

function hex(bytes: Uint8Array): string {
  return [...bytes].map((value) => value.toString(16).padStart(2, '0')).join('')
}
