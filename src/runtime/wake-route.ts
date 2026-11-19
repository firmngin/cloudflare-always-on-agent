export type RuntimeRoute = 'health' | 'wake' | 'method_not_allowed' | 'not_found'

export type WakeCallbackSource = 'worker_base_url' | 'wake_callback_url'

/** Both wake settings the environment may carry, in precedence order. */
export interface WakeCallbackEnv {
  /** Preferred: the Worker's own public HTTPS origin, nothing else. */
  baseUrl?: string
  /** Legacy: the full signed-ingress URL ending in /v1/wake. */
  callbackUrl?: string
}

export interface ResolvedWakeCallback {
  url: string | null
  source: WakeCallbackSource | null
}

/**
 * A base must be a bare origin. The literal comparison is what rejects a path
 * the URL parser would normalize away (`/a/..`, `/%2e%2e`) instead of quietly
 * accepting it as root.
 */
function normalizeWorkerBaseUrl(raw: string): string | null {
  if (!raw) return null
  let parsed: URL
  try {
    parsed = new URL(raw)
  } catch {
    return null
  }
  if (parsed.protocol !== 'https:' || parsed.username || parsed.password) return null
  const literal = raw.toLowerCase()
  if (literal !== parsed.origin && literal !== `${parsed.origin}/`) return null
  return parsed.origin
}

function normalizeWakeCallbackUrl(raw: string): string | null {
  if (!raw) return null
  try {
    const parsed = new URL(raw)
    const valid = parsed.protocol === 'https:' && parsed.pathname === '/v1/wake' &&
      !parsed.search && !parsed.hash && !parsed.username && !parsed.password
    return valid ? `${parsed.origin}${parsed.pathname}` : null
  } catch {
    return null
  }
}

/**
 * Firmngin still receives the full `wake_url` at handshake time; only the local
 * input changed. WORKER_BASE_URL is provisioned once and authoritative, so an
 * invalid explicit base fails closed rather than reviving a stale callback URL.
 */
export function resolveWakeCallbackUrl(env: WakeCallbackEnv): ResolvedWakeCallback {
  const base = String(env.baseUrl ?? '').trim()
  if (base) {
    const origin = normalizeWorkerBaseUrl(base)
    return origin
      ? { url: `${origin}/v1/wake`, source: 'worker_base_url' }
      : { url: null, source: null }
  }
  const legacy = normalizeWakeCallbackUrl(String(env.callbackUrl ?? '').trim())
  return legacy
    ? { url: legacy, source: 'wake_callback_url' }
    : { url: null, source: null }
}

export function classifyRuntimeRoute(method: string, path: string, search = ''): RuntimeRoute {
  const normalizedMethod = method.toUpperCase()
  if (search !== '') return 'not_found'
  if (path === '/v1/wake') {
    return normalizedMethod === 'POST' ? 'wake' : 'method_not_allowed'
  }
  if (path === '/' && normalizedMethod === 'GET') return 'health'
  return 'not_found'
}
