/**
 * Backend endpoint + credential resolution shared by the Worker entrypoint and
 * the Durable Object runtime.
 *
 * Kept in its own module so `src/index.ts` and `src/durable/agent.ts` read the
 * exact same rules: a development deployment may override the backend through
 * ENVIRONMENT="development" + URL_BACKEND_DEV, and a wizard placeholder token
 * must never be treated as configured (it fails every call with an opaque 401).
 */

export interface BackendEnvLike {
  URL_BACKEND: string
  ENVIRONMENT?: string
  URL_BACKEND_DEV?: string
  WORKER_TOKEN?: string
}

export function trimSlash(value: string): string {
  return String(value || '').replace(/\/+$/, '')
}

/**
 * The dev override only applies when the deployment is explicitly marked as
 * development, so a stray URL_BACKEND_DEV on production cannot redirect it.
 */
export function resolveBackendUrl(env: BackendEnvLike): string {
  const isDev = String(env.ENVIRONMENT || '').trim().toLowerCase() === 'development'
  const devUrl = String(env.URL_BACKEND_DEV || '').trim()
  if (isDev && devUrl) return trimSlash(devUrl)
  return trimSlash(String(env.URL_BACKEND || '').trim())
}

/**
 * The wizard pre-fills secrets from .dev.vars.example, so a user who clicks
 * through without editing deploys the placeholder verbatim. That token passes a
 * mere emptiness check and then fails every call with an opaque 401, which reads
 * like "the Worker is broken" instead of "the secret was never filled in".
 */
export function isPlaceholderToken(value: string): boolean {
  const token = String(value || '').trim()
  if (!token) return true
  return /^replace_with/i.test(token) || /\breplace_me\b/i.test(token)
}

export function runtimePath(apiUrl: string, path: string): string {
  return `${trimSlash(apiUrl)}/api/v1/agent-runtime/worker${path}`
}

/** JSON POST/GET helper against the worker runtime surface; returns parsed body. */
export async function runtimeFetch(
  apiUrl: string,
  workerToken: string,
  path: string,
  init: RequestInit = {},
): Promise<{ ok: boolean, status: number, body: any }> {
  const response = await fetch(runtimePath(apiUrl, path), {
    ...init,
    headers: {
      Authorization: `Bearer ${workerToken}`,
      'Content-Type': 'application/json',
      ...(init.headers || {}),
    },
  })
  let body: any = null
  try {
    body = await response.json()
  } catch {
    body = null
  }
  return { ok: response.ok, status: response.status, body }
}
