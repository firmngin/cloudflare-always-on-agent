/**
 * Local mock backend for running the Worker entrypoint under `node --test`.
 *
 * Everything the Worker calls is answered here: the Firmngin runtime API
 * (handshake, heartbeat, deployment config, inbox, claim, MCP token, run events,
 * terminal run PATCH), the MCP server, and the model provider. No network, no
 * real provider key, no live backend.
 */
import worker from '../../src/index.ts'

export const API = 'https://api.test.local'
export const MCP_URL = 'https://mcp.test.local/mcp'
export const MODEL_BASE = 'https://provider.test.local/api/v1'
export const AGENT_ID = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
export const WORKSPACE_ID = '7492252c-72a3-4d68-befb-07da8484cc23'
export const RUN_ID = '11111111-2222-3333-4444-555555555555'
export const CONVERSATION_ID = '99999999-2222-3333-4444-555555555555'
export const MESSAGE_ID = 41
export const MODEL_API_KEY = 'sk-test-key-never-leaked'

/** One OpenAI chat-completions SSE body carrying either text or a tool call. */
export function completionText(text: string): string {
  return sse([{ choices: [{ delta: { content: text } }] }])
}

export function completionToolCall(id: string, name: string, args: Record<string, unknown>, text = ''): string {
  return sse([
    ...(text ? [{ choices: [{ delta: { content: text } }] }] : []),
    { choices: [{ delta: { tool_calls: [{ index: 0, id, function: { name, arguments: '' } }] } }] },
    { choices: [{ delta: { tool_calls: [{ index: 0, function: { arguments: JSON.stringify(args) } }] } }] },
  ])
}

function sse(frames: unknown[]): string {
  return `${frames.map((frame) => `data: ${JSON.stringify(frame)}\n\n`).join('')}data: [DONE]\n\n`
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } })
}

export interface RecordedRequest {
  method: string
  url: string
  body: string
}

export interface RecordedPatch {
  url: string
  status: number
  body: Record<string, unknown>
}

export interface MockBackend {
  env: Record<string, string>
  requests: RecordedRequest[]
  /** Every event the Worker posted for the run, in delivery order. */
  events: Array<{ type: string, content: string, seq: number }>
  /** Model completions requested: one per assistant turn. */
  completions: string[]
  /** MCP tools/call names, in call order. */
  toolCalls: string[]
  /** Terminal run reports the Worker sent, with the backend's answer. */
  patches: RecordedPatch[]
  logs: Array<Record<string, any>>
  restore: () => void
}

/**
 * Installs the mock backend on `globalThis.fetch` and captures console output.
 * `completions` are served in order; a 401 answer is served when
 * `options.modelStatus` is set (invalid provider key).
 */
export function installMockBackend(options: {
  engine?: 'pi' | 'builtin'
  completions?: string[]
  modelStatus?: number
  toolOutput?: string
}): MockBackend {
  const requests: RecordedRequest[] = []
  const events: MockBackend['events'] = []
  const completions: string[] = []
  const toolCalls: string[] = []
  const patches: RecordedPatch[] = []
  const logs: Array<Record<string, any>> = []
  const queue = [...(options.completions || [])]
  const toolOutput = options.toolOutput || 'living-room-sensor (online), garage-door (offline)'
  let polled = false

  const claim = {
    message: { id: MESSAGE_ID, role: 'user', content: 'Which devices are online?', created_at: new Date().toISOString() },
    run_id: RUN_ID,
    conversation_id: CONVERSATION_ID,
    history: [
      { role: 'user', content: 'hello' },
      { role: 'assistant', content: 'Hi! How can I help?' },
    ],
    lane: 'interactive',
    task_prompt: 'Answer with the workspace device list.',
    platform_prompt: 'You are the Firmngin data agent. Use the tools you are granted.',
    model: 'openai/gpt-4o-mini',
    provider: 'openrouter',
    model_base_url: MODEL_BASE,
    model_protocol: 'openai',
    config_revision: 3,
    mcp_enabled: true,
  }

  const originalFetch = globalThis.fetch
  const originalLog = console.log
  const originalError = console.error
  const originalWarn = console.warn
  const capture = (level: string) => (...args: unknown[]) => {
    if (args.length === 1 && typeof args[0] === 'object' && args[0] !== null) logs.push(args[0] as Record<string, any>)
    else logs.push({ level, message: args.map(String).join(' ') })
  }
  console.log = capture('log')
  console.error = capture('error')
  console.warn = capture('warn')

  globalThis.fetch = async (input: any, init: any = {}): Promise<Response> => {
    const url = typeof input === 'string' ? input : String(input?.url || input)
    const method = String(init?.method || 'GET').toUpperCase()
    const body = typeof init?.body === 'string' ? init.body : ''
    requests.push({ method, url, body })

    if (url.startsWith(MODEL_BASE)) {
      completions.push(body)
      if (options.modelStatus) {
        return json({ error: { message: 'User not found.' } }, options.modelStatus)
      }
      const next = queue.shift()
      if (!next) return json({ error: { message: 'mock backend ran out of completions' } }, 500)
      return new Response(next, { status: 200, headers: { 'content-type': 'text/event-stream' } })
    }

    if (url.startsWith(MCP_URL)) {
      const payload = JSON.parse(body || '{}')
      if (payload.method === 'initialize') {
        return json({ jsonrpc: '2.0', id: payload.id, result: { protocolVersion: '2025-03-26', capabilities: {} } })
      }
      if (payload.method === 'notifications/initialized') return new Response(null, { status: 202 })
      if (payload.method === 'tools/list') {
        return json({
          jsonrpc: '2.0',
          id: payload.id,
          result: {
            tools: [{
              name: 'list_devices',
              description: 'List the devices of this workspace (read-only).',
              inputSchema: {
                type: 'object',
                properties: { limit: { type: 'integer', description: 'Maximum devices to return.' } },
              },
            }],
          },
        })
      }
      if (payload.method === 'tools/call') {
        toolCalls.push(payload.params?.name)
        return json({
          jsonrpc: '2.0',
          id: payload.id,
          result: { content: [{ type: 'text', text: toolOutput }] },
        })
      }
      return json({ jsonrpc: '2.0', id: payload.id, error: { message: `unknown method ${payload.method}` } })
    }

    const path = url.slice(API.length)
    if (path.startsWith('/api/v1/agent-runtime/worker/heartbeat')) {
      return json({ data: { workspace_id: WORKSPACE_ID, last_heartbeat_at: new Date().toISOString() } })
    }
    if (path.startsWith('/api/v1/agent-runtime/worker/handshake')) {
      return json({ data: { workspace_id: WORKSPACE_ID } })
    }
    if (path.startsWith('/api/v1/agent-runtime/worker/deployment/config')) {
      return json({ data: { workspace_id: WORKSPACE_ID, agents: [] } })
    }
    if (path.startsWith('/api/v1/agent-runtime/worker/inbox')) {
      // First poll hands out one queued turn; the next one is an immediate empty
      // response, which ends the drain until the next cron tick.
      if (polled) return json({ data: [] })
      polled = true
      return json({
        data: [{
          agent_id: AGENT_ID,
          message: { id: MESSAGE_ID, role: 'user', content: claim.message.content },
          run_id: RUN_ID,
          conversation_id: CONVERSATION_ID,
          lane: 'interactive',
        }],
      })
    }
    if (/\/inbox\/\d+\/claim$/.test(path)) return json({ data: claim })
    if (path.endsWith('/mcp-token')) {
      return json({ data: { access_token: 'mcp-test-token', mcp_url: MCP_URL } })
    }
    if (path.endsWith('/events')) {
      const parsed = JSON.parse(body || '{}')
      for (const event of parsed.events || []) events.push(event)
      return json({ ok: true })
    }
    if (method === 'PATCH' && path.startsWith('/api/v1/agent-runtime/worker/agents/')) {
      const report = JSON.parse(body || '{}')
      patches.push({ url, status: 200, body: report })
      return json({ data: { status: report.status } })
    }
    return json({ error: 'UNEXPECTED_REQUEST', path }, 404)
  }

  return {
    env: {
      URL_BACKEND: API,
      WORKER_TOKEN: 'fng_aow_mock_workspace_token',
      MODEL_API_KEY,
      MODEL_BASE_URL: MODEL_BASE,
      MODEL_MAX_TOKENS: '512',
      MAX_STEPS: '8',
      SUBREQUEST_BUDGET: '200',
      ...(options.engine ? { AGENT_ENGINE: options.engine } : {}),
    },
    requests,
    events,
    completions,
    toolCalls,
    patches,
    logs,
    restore() {
      globalThis.fetch = originalFetch
      console.log = originalLog
      console.error = originalError
      console.warn = originalWarn
    },
  }
}

/** Runs one cron invocation and waits for its waitUntil work, exactly like the runtime does. */
export async function runScheduled(env: Record<string, string>): Promise<void> {
  const pending: Array<Promise<unknown>> = []
  const ctx = {
    waitUntil(promise: Promise<unknown>) {
      pending.push(promise)
    },
    passThroughOnException() {},
  }
  await (worker as any).scheduled(
    { cron: '* * * * *', scheduledTime: Date.now(), noRetry() {} },
    env,
    ctx,
  )
  await Promise.all(pending)
}

/** The Worker's log entry for the invocation's subrequest totals. */
export function invocationSubrequests(logs: Array<Record<string, any>>): number | undefined {
  return logs.find((entry) => entry.event === 'runtime.invocation.complete')?.subrequests_used
}
