import { isPlaceholderToken, resolveBackendUrl, runtimePath, withRequestTimeout, type BackendEnvLike } from './endpoints.ts'
import { fetchWithBudget, type SubrequestBudget } from './subrequest-budget.ts'

export interface RuntimeTrace {
  invocation_id: string
  worker_version: string
  trigger: 'scheduled' | 'fetch'
  environment: 'development' | 'production'
  backend_origin: string
  backend_base_path: string
  backend_source: 'URL_BACKEND_DEV' | 'URL_BACKEND'
  backend_configured: boolean
  backend_valid: boolean
  token_configured: boolean
  dev_override_ignored: boolean
}

/** Never log URL credentials, query strings, fragments, tokens, or raw bodies. */
export function runtimeTrace(env: BackendEnvLike, version: string, trigger: RuntimeTrace['trigger']): RuntimeTrace {
  const backend = resolveBackendUrl(env)
  const development = String(env.ENVIRONMENT || '').trim().toLowerCase() === 'development'
  let origin = '', path = '', valid = false
  try {
    const url = new URL(backend)
    valid = ['http:', 'https:'].includes(url.protocol) && !url.username && !url.password && !url.search && !url.hash
    origin = url.origin
    path = url.pathname.replace(/fng_[a-z0-9_-]+/gi, '[redacted]')
  } catch { /* The diagnostic flags identify a malformed base URL. */ }
  return {
    invocation_id: crypto.randomUUID(), worker_version: version, trigger,
    environment: development ? 'development' : 'production',
    backend_origin: origin, backend_base_path: path,
    backend_source: development && String(env.URL_BACKEND_DEV || '').trim() ? 'URL_BACKEND_DEV' : 'URL_BACKEND',
    backend_configured: Boolean(backend), backend_valid: valid,
    token_configured: !isPlaceholderToken(env.WORKER_TOKEN || ''),
    dev_override_ignored: !development && Boolean(String(env.URL_BACKEND_DEV || '').trim()),
  }
}

export function logRuntimeTrace(trace: RuntimeTrace, event: string, fields: Record<string, unknown> = {}, failed = false) {
  const entry = { event, ...trace, ...fields }
  if (failed) console.error(entry)
  else console.log(entry)
}

function safeId(value: string | null): string {
  return value && /^[a-f0-9-]{16,64}$/i.test(value) ? value : ''
}

async function boundedJson(response: Response): Promise<Record<string, any> | null> {
  if (!response.body) return null
  if (!response.headers.get('content-type')?.includes('application/json')) {
    await response.body.cancel()
    return null
  }
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let text = '', bytes = 0
  try {
    while (true) {
      const part = await reader.read()
      if (part.done) break
      bytes += part.value.byteLength
      if (bytes > 8192) { await reader.cancel(); return null }
      text += decoder.decode(part.value, { stream: true })
    }
    try {
      const value = JSON.parse(text + decoder.decode())
      return value && typeof value === 'object' && !Array.isArray(value) ? value : null
    } catch { return null }
  } finally { reader.releaseLock() }
}

/** Registration is best-effort, but every attempt gets a bounded, correlated outcome. */
export async function traceRegistration(
  trace: RuntimeTrace, backend: string, token: string,
  kind: 'heartbeat' | 'handshake', payload: Record<string, string>,
  budget: SubrequestBudget,
): Promise<void> {
  const traceId = crypto.randomUUID()
  const started = Date.now()
  const fields = { trace_id: traceId, endpoint: `/api/v1/agent-runtime/worker/${kind}` }
  logRuntimeTrace(trace, `runtime.${kind}.start`, fields)
  let status = 0, backendRequestId = '', cfRay = '', responseFormat = ''
  try {
    await withRequestTimeout(async (signal) => {
      // Registration is diagnostic: it must never take the headroom a turn's
      // terminal report needs, so it spends the best-effort tier.
      const response = await fetchWithBudget(budget, runtimePath(backend, `/${kind}`), {
        method: 'POST', signal,
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', 'X-Trace-Id': traceId },
        body: JSON.stringify(payload),
      }, 'bestEffort')
      status = response.status
      backendRequestId = safeId(response.headers.get('x-request-id'))
      const ray = response.headers.get('cf-ray') || ''
      cfRay = /^[a-f0-9]{8,32}-[a-z]{3}$/i.test(ray) ? ray : ''
      responseFormat = response.headers.get('content-type')?.includes('application/json') ? 'json' : 'non_json'
      const body = await boundedJson(response)
      const code = typeof body?.error === 'string' && /^[A-Z][A-Z0-9_]{1,79}$/.test(body.error) ? body.error : ''
      const timestamp = typeof body?.data?.last_heartbeat_at === 'string' ? body.data.last_heartbeat_at : ''
      const heartbeatAt = timestamp.length < 40 && Number.isFinite(Date.parse(timestamp)) ? timestamp : ''
      logRuntimeTrace(trace, `runtime.${kind}.response`, {
        ...fields, status, duration_ms: Date.now() - started,
        backend_request_id: backendRequestId, cf_ray: cfRay, response_format: responseFormat,
        error_code: code, ok: response.ok,
        workspace_id: safeId(typeof body?.data?.workspace_id === 'string' ? body.data.workspace_id : null),
        worker_id: safeId(typeof body?.data?.worker_id === 'string' ? body.data.worker_id : null),
        ...(kind === 'heartbeat' ? { persistence_ack: Boolean(heartbeatAt), last_heartbeat_at: heartbeatAt } : {}),
        legacy_endpoint: kind === 'handshake' && status === 404,
      }, !response.ok || (kind === 'heartbeat' && !heartbeatAt))
    }, 15_000, 'registration_timeout')
  } catch (error) {
    // Error messages may embed a URL or credential: log only a safe category.
    const timeout = error instanceof Error && error.message === 'registration_timeout'
    logRuntimeTrace(trace, `runtime.${kind}.failed`, {
      ...fields, status, duration_ms: Date.now() - started,
      backend_request_id: backendRequestId, cf_ray: cfRay, response_format: responseFormat,
      failure: timeout ? 'timeout' : 'network_or_response_failure',
    }, true)
  }
}
