const encoder = new TextEncoder()

export const WAKE_REPLAY_WINDOW_SECONDS = 300

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
