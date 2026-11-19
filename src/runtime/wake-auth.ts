const encoder = new TextEncoder()

export const WAKE_REPLAY_WINDOW_SECONDS = 300

/**
 * Domain label for the derived signing key. Fixed and versioned so the key can
 * never collide with the Worker token's own use (backend authentication) or
 * with a future derivation, and so a change of purpose is a visible change
 * here instead of a silent key rotation.
 */
export const WAKE_SIGNING_KEY_DOMAIN = 'firmngin/data-agent/wake-signing-key/v1'

/**
 * The backend -> Worker signing key, derived from the Worker token so a new
 * deployment has one secret fewer to invent (WORKER_TOKEN + MODEL_API_KEY).
 * HMAC-SHA-256 keyed by the token with the domain label as the message:
 * deterministic on every isolate, never equal to the token, and different
 * after a token rotation (the next handshake re-registers it). Hex output is
 * 64 bytes, inside the backend's 32-256 byte wake_secret window.
 */
export async function deriveWakeSigningKey(workerToken: string): Promise<string> {
  const token = String(workerToken || '').trim()
  if (!token) return ''
  const key = await crypto.subtle.importKey(
    'raw',
    encoder.encode(token),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  )
  const mac = await crypto.subtle.sign('HMAC', key, encoder.encode(WAKE_SIGNING_KEY_DOMAIN))
  return hex(new Uint8Array(mac))
}

export interface WakeAuthBinding {
  deploymentId: string
  workspaceId: string
  nonce: string
  timestamp: number
}

export function wakeCanonical(
  method: string,
  path: string,
  timestamp: string,
  nonce: string,
  bodyHash: string,
  deploymentId: string,
  workspaceId: string,
): string {
  return [method, path, timestamp, nonce, bodyHash, deploymentId, workspaceId].join('\n')
}

export async function verifyWakeSignature(
  request: Request,
  body: Uint8Array,
  secret: string,
  nowSeconds = Math.floor(Date.now() / 1000),
): Promise<WakeAuthBinding> {
  const timestampRaw = request.headers.get('X-Firmngin-Timestamp') || ''
  const nonce = request.headers.get('X-Firmngin-Nonce') || ''
  const deploymentId = request.headers.get('X-Firmngin-Deployment-ID') || ''
  const workspaceId = request.headers.get('X-Firmngin-Workspace-ID') || ''
  const bodyHashHeader = (request.headers.get('X-Firmngin-Body-SHA256') || '').toLowerCase()
  const signatureHeader = request.headers.get('X-Firmngin-Signature') || ''
  const timestamp = Number(timestampRaw)
  if (!Number.isInteger(timestamp) || Math.abs(nowSeconds - timestamp) > WAKE_REPLAY_WINDOW_SECONDS) {
    throw new WakeAuthError(401, 'wake timestamp is outside the replay window')
  }
  if (!/^[A-Za-z0-9_-]{16,128}$/.test(nonce)) {
    throw new WakeAuthError(401, 'wake nonce is invalid')
  }
  if (!isUuid(deploymentId) || !isUuid(workspaceId)) {
    throw new WakeAuthError(401, 'wake deployment binding is invalid')
  }
  if (!/^v1=[0-9a-f]{64}$/i.test(signatureHeader) || !/^[0-9a-f]{64}$/i.test(bodyHashHeader)) {
    throw new WakeAuthError(401, 'wake signature is invalid')
  }

  const digest = await crypto.subtle.digest('SHA-256', body)
  const bodyHash = hex(new Uint8Array(digest))
  if (bodyHash !== bodyHashHeader) {
    throw new WakeAuthError(401, 'wake body hash mismatch')
  }
  const canonical = wakeCanonical(
    request.method.toUpperCase(),
    new URL(request.url).pathname,
    timestampRaw,
    nonce,
    bodyHash,
    deploymentId,
    workspaceId,
  )
  const key = await crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['verify'],
  )
  const valid = await crypto.subtle.verify(
    'HMAC',
    key,
    unhex(signatureHeader.slice(3)),
    encoder.encode(canonical),
  )
  if (!valid) throw new WakeAuthError(401, 'wake signature is invalid')
  return { deploymentId, workspaceId, nonce, timestamp }
}

export class WakeAuthError extends Error {
  constructor(readonly status: number, message: string) {
    super(message)
  }
}

function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)
}

function hex(bytes: Uint8Array): string {
  return [...bytes].map((value) => value.toString(16).padStart(2, '0')).join('')
}

function unhex(value: string): Uint8Array {
  const result = new Uint8Array(value.length / 2)
  for (let i = 0; i < value.length; i += 2) result[i / 2] = Number.parseInt(value.slice(i, i + 2), 16)
  return result
}
