export type RuntimeRoute = 'health' | 'wake' | 'method_not_allowed' | 'not_found'

export function classifyRuntimeRoute(method: string, path: string, search = ''): RuntimeRoute {
  const normalizedMethod = method.toUpperCase()
  if (search !== '') return 'not_found'
  if (path === '/v1/wake') {
    return normalizedMethod === 'POST' ? 'wake' : 'method_not_allowed'
  }
  if (path === '/' && normalizedMethod === 'GET') return 'health'
  return 'not_found'
}
