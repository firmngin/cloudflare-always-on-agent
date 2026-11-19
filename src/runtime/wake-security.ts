export interface WakeSecurityState {
  nonces: Record<string, number>
  events: Record<string, { status: 'running' | 'done', expiresAt: number }>
  requests: number[]
}

export interface WakeSecurityDecision {
  accepted: boolean
  duplicate: boolean
  duplicateCompleted: boolean
  rateLimited: boolean
  state: WakeSecurityState
}

const replayWindowMs = 5 * 60_000
const eventLeaseMs = 6 * 60_000
const rateWindowMs = 60_000
const maxRequestsPerMinute = 30
const maxEntries = 256

export function authorizeWakeState(
  current: WakeSecurityState | undefined,
  nonce: string,
  eventId: string,
  now: number,
): WakeSecurityDecision {
  const state: WakeSecurityState = current || { nonces: {}, events: {}, requests: [] }
  state.nonces = pruneRecord(state.nonces, now - replayWindowMs)
  state.events = Object.fromEntries(
    Object.entries(state.events).filter(([, value]) => value.status === 'done' || value.expiresAt > now),
  )
  state.requests = state.requests.filter((value) => value > now - rateWindowMs).slice(-maxEntries)

  if (state.nonces[nonce]) return { accepted: false, duplicate: true, duplicateCompleted: false, rateLimited: false, state }
  state.nonces[nonce] = now
  state.requests.push(now)
  if (state.requests.length > maxRequestsPerMinute) {
    return { accepted: false, duplicate: false, duplicateCompleted: false, rateLimited: true, state: boundState(state) }
  }
  const event = state.events[eventId]
  if (event?.status === 'done') {
    return { accepted: false, duplicate: true, duplicateCompleted: true, rateLimited: false, state: boundState(state) }
  }
  if (event?.status === 'running' && event.expiresAt > now) {
    return { accepted: false, duplicate: true, duplicateCompleted: false, rateLimited: false, state: boundState(state) }
  }
  state.events[eventId] = { status: 'running', expiresAt: now + eventLeaseMs }
  return { accepted: true, duplicate: false, duplicateCompleted: false, rateLimited: false, state: boundState(state) }
}

export function completeWakeState(state: WakeSecurityState, eventId: string, now: number): WakeSecurityState {
  state.events[eventId] = { status: 'done', expiresAt: now + replayWindowMs }
  return boundState(state)
}

export function releaseWakeState(state: WakeSecurityState, eventId: string): WakeSecurityState {
  delete state.events[eventId]
  return state
}

function pruneRecord(input: Record<string, number>, cutoff: number): Record<string, number> {
  return Object.fromEntries(Object.entries(input).filter(([, value]) => value > cutoff))
}

function boundState(state: WakeSecurityState): WakeSecurityState {
  state.nonces = Object.fromEntries(Object.entries(state.nonces).sort((a, b) => b[1] - a[1]).slice(0, maxEntries))
  state.events = Object.fromEntries(Object.entries(state.events).sort((a, b) => b[1].expiresAt - a[1].expiresAt).slice(0, maxEntries))
  return state
}
