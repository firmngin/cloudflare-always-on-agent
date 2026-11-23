/**
 * pi StreamFn adapter over the Worker's own BYOK model providers.
 *
 * The template's provider adapters (OpenAI chat completions + Anthropic
 * Messages streaming, moved here verbatim from src/index.ts so both engines
 * share them) speak the provider wire protocols with the user's own key. On
 * top of them this module implements pi's StreamFn contract
 * (@earendil-works/pi-agent-core): given a normalized pi transcript, call the
 * provider and emit the exact AssistantMessageEvent protocol the pi agent loop
 * expects — `start` first, partial updates, then a terminal
 * `done`/`error` carrying the final AssistantMessage.
 *
 * StreamFn contract (never throws): every request/model/runtime failure is
 * encoded in the returned stream as stopReason "error" + errorMessage on the
 * final AssistantMessage. When the failure is a template ApiError (status +
 * error code), it is additionally preserved as a message diagnostic so the
 * engine can rethrow an equivalent error and runTurn reports the same
 * error_code the builtin engine would have.
 *
 * BYOK: the provider key lives only in Worker secrets (module state set by
 * applyModelRuntimeEnv from the env binding); it is never logged and never
 * sent anywhere except the model provider endpoint itself.
 */

import {
	contentText,
	createAssistantMessageEventStream,
	getCurrentTools,
	type Api,
	type AssistantMessage,
	type JsonObject,
	type Message,
	type Model,
	type StopReason,
	type TextContent,
	type ThinkingContent,
	type ToolCall,
	type TranscriptContext,
	type Usage,
} from '@earendil-works/pi-ai'
import type { StreamFn } from '@earendil-works/pi-agent-core'
import { safeJson, type McpTool } from './tools'
import { fetchWithBudget, type SubrequestBudget } from '../runtime/subrequest-budget'

export class ApiError extends Error {
  constructor(readonly status: number, message: string, readonly errorCode = '') {
    super(message)
  }
}

export type Role = 'system' | 'user' | 'assistant' | 'tool'

export interface ChatMessage {
  role: Role
  content: string
  tool_calls?: Array<{ id: string, type: 'function', function: { name: string, arguments: string } }>
  tool_call_id?: string
  name?: string
}

export async function* sseDataLines(response: Response, signal?: AbortSignal): AsyncGenerator<string> {
  const reader = response.body?.getReader()
  if (!reader) return
  const decoder = new TextDecoder()
  let buffer = ''
  while (true) {
    if (signal?.aborted) {
      await reader.cancel(signal.reason).catch(() => {})
      throw signal.reason instanceof Error ? signal.reason : new Error('Request aborted')
    }
    const { done, value } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true })
    let newline: number
    while ((newline = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, newline).trim()
      buffer = buffer.slice(newline + 1)
      if (line.startsWith('data:')) yield line.slice(5).trim()
    }
  }
}

interface ToolCallAccumulator {
  id: string
  name: string
  arguments: string
}

export interface ModelTurnResult {
  content: string
  toolCalls: Array<{ id: string, name: string, arguments: string }>
}

export type ModelProtocol = 'openai' | 'anthropic'

const ANTHROPIC_PROTOCOL_PROVIDERS = new Set(['anthropic', 'claude-compatible', 'claude'])

// Per-invocation configuration captured from the env binding; module-scope so
// every helper reads it without threading the env object through each call.
// (Moved here from src/index.ts with the adapters that consume it.)
let modelApiKey = ''
let modelBase = ''
let modelMaxTokens = ''

/** Model-provider subset of the Worker env binding; applied per invocation. */
export interface ModelRuntimeEnv {
  MODEL_API_KEY?: string
  MODEL_BASE_URL?: string
  MODEL_MAX_TOKENS?: string
}

/** Called from applyEnv() so the adapters below read the same values they read when they lived in src/index.ts. */
export function applyModelRuntimeEnv(env: ModelRuntimeEnv): void {
  modelApiKey = env.MODEL_API_KEY || ''
  modelBase = env.MODEL_BASE_URL || ''
  modelMaxTokens = env.MODEL_MAX_TOKENS || ''
}

// The claim carries the protocol from the Firmngin registry; when it is
// missing (older backend), the provider id and finally the base URL host
// decide, so a user pointing MODEL_BASE_URL at api.anthropic.com still gets
// the right wire format.
export function resolveModelProtocol(claim: { model_protocol?: string, provider?: string, model_base_url?: string }): ModelProtocol {
  const fromClaim = String(claim.model_protocol || '').trim().toLowerCase()
  if (fromClaim === 'anthropic' || fromClaim === 'openai') return fromClaim
  const provider = String(claim.provider || '').trim().toLowerCase()
  if (ANTHROPIC_PROTOCOL_PROVIDERS.has(provider)) return 'anthropic'
  const override = trimSlash(String(claim.model_base_url || modelBase || ''))
  if (override.includes('anthropic')) return 'anthropic'
  return 'openai'
}

function trimSlash(value: string): string {
  return String(value || '').replace(/\/+$/, '')
}

function anthropicMaxTokens(): number {
  const parsed = Number(modelMaxTokens)
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 8192
}

export async function callModel(
  budget: SubrequestBudget,
  model: string,
  messages: ChatMessage[],
  tools: McpTool[],
  onDelta: (text: string) => Promise<void>,
  // Per-agent base URL from the claim; the env var stays the fallback so
  // older backends and custom deployments keep working.
  baseUrlOverride?: string,
  protocol: ModelProtocol = 'openai',
  signal?: AbortSignal,
  runId?: string,
  // Provider reasoning deltas (DeepSeek/OpenRouter `reasoning`, Anthropic
  // `thinking_delta`). Optional: a provider that does not reason never calls it.
  onReasoning?: (text: string) => Promise<void>,
): Promise<ModelTurnResult> {
  if (protocol === 'anthropic') {
    return callAnthropicModel(budget, model, messages, tools, onDelta, baseUrlOverride, signal, runId, onReasoning)
  }
  return callOpenAIModel(budget, model, messages, tools, onDelta, baseUrlOverride, signal, runId, onReasoning)
}

async function callOpenAIModel(
  budget: SubrequestBudget,
  model: string,
  messages: ChatMessage[],
  tools: McpTool[],
  onDelta: (text: string) => Promise<void>,
  baseUrlOverride?: string,
  signal?: AbortSignal,
  runId?: string,
  onReasoning?: (text: string) => Promise<void>,
): Promise<ModelTurnResult> {
  const base = trimSlash(baseUrlOverride || modelBase || 'https://openrouter.ai/api/v1')
  const body: Record<string, unknown> = { model, messages, stream: true }
  if (tools.length) {
    body.tools = tools.map((tool) => ({
      type: 'function',
      function: { name: tool.name, description: tool.description || '', parameters: tool.inputSchema || { type: 'object' } },
    }))
  }

  const requestStartedAt = Date.now()
  const response = await fetchWithBudget(budget, `${base}/chat/completions`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${modelApiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
    signal,
  }, 'essential')
  if (!response.ok) {
    const detail: any = await response.json().catch(() => null)
    const message = detail?.error?.message || detail?.message || `Model provider returned ${response.status}`
    if (response.status === 401 || response.status === 403) {
      throw new ApiError(response.status, message, 'MODEL_AUTH_FAILED')
    }
    throw new ApiError(response.status, message, 'MODEL_REQUEST_FAILED')
  }

  let content = ''
  const calls = new Map<number, ToolCallAccumulator>()
  let firstDeltaAt = 0
  for await (const data of sseDataLines(response, signal)) {
    if (!data || data === '[DONE]') continue
    let chunk: any
    try {
      chunk = JSON.parse(data)
    } catch {
      continue
    }
    const delta = chunk?.choices?.[0]?.delta
    if (!delta) continue
    if (typeof delta.content === 'string' && delta.content.length) {
      if (!firstDeltaAt) {
        firstDeltaAt = Date.now()
        console.log({
          event: 'run.stage',
          run_id: runId,
          stage: 'provider_first_delta',
          duration_ms: firstDeltaAt - requestStartedAt,
        })
      }
      content += delta.content
      await onDelta(delta.content)
    }
    // Reasoning models stream their thinking on a side channel that never
    // reaches `content`; forward it only when the caller asked for it.
    const reasoning = delta.reasoning_content ?? delta.reasoning
    if (onReasoning && typeof reasoning === 'string' && reasoning.length) {
      await onReasoning(reasoning)
    }
    for (const call of delta.tool_calls || []) {
      const index = Number(call.index ?? 0)
      const current = calls.get(index) || { id: '', name: '', arguments: '' }
      if (call.id) current.id = call.id
      if (call.function?.name) current.name += call.function.name
      if (call.function?.arguments) current.arguments += call.function.arguments
      calls.set(index, current)
    }
  }
  return {
    content,
    toolCalls: [...calls.values()].filter((call) => call.name),
  }
}

// ---------------------------------------------------------------------------
// Anthropic Messages adapter
// ---------------------------------------------------------------------------

// Anthropic's Messages API is a different wire protocol from OpenAI chat
// completions: x-api-key auth, a required max_tokens, the system prompt as a
// top-level parameter, tool calls as content blocks, and its own SSE events.
// The internal ChatMessage shape stays OpenAI-ish; these helpers translate in
// both directions so neither engine needs protocol awareness.

function anthropicEndpoint(baseUrlOverride?: string): string {
  const base = trimSlash(baseUrlOverride || modelBase || 'https://api.anthropic.com')
  return `${base}/v1/messages`
}

interface AnthropicContentBlock {
  type: string
  text?: string
  id?: string
  name?: string
  input?: Record<string, unknown>
  tool_use_id?: string
  content?: string | Array<{ type: string, text?: string }>
}

function toAnthropicMessages(messages: ChatMessage[]): { system: string, messages: Array<{ role: string, content: string | AnthropicContentBlock[] }> } {
  const systemParts: string[] = []
  const out: Array<{ role: string, content: string | AnthropicContentBlock[] }> = []

  for (const message of messages) {
    if (message.role === 'system') {
      if (message.content) systemParts.push(message.content)
      continue
    }
    if (message.role === 'tool') {
      // A tool result must arrive as a user turn holding tool_result blocks.
      // Consecutive tool results merge into one user turn, which is exactly
      // how the OpenAI loop emits them (one per executed call).
      const block: AnthropicContentBlock = {
        type: 'tool_result',
        tool_use_id: message.tool_call_id || '',
        content: message.content || '',
      }
      const previous = out[out.length - 1]
      if (previous && previous.role === 'user' && Array.isArray(previous.content)) {
        previous.content.push(block)
      } else {
        out.push({ role: 'user', content: [block] })
      }
      continue
    }
    if (message.role === 'assistant' && message.tool_calls?.length) {
      const blocks: AnthropicContentBlock[] = []
      if (message.content) blocks.push({ type: 'text', text: message.content })
      for (const call of message.tool_calls) {
        blocks.push({
          type: 'tool_use',
          id: call.id,
          name: call.function.name,
          input: safeJson(call.function.arguments),
        })
      }
      out.push({ role: 'assistant', content: blocks })
      continue
    }
    const role = message.role === 'assistant' ? 'assistant' : 'user'
    if (message.content) out.push({ role, content: message.content })
  }
  return { system: systemParts.join('\n\n'), messages: out }
}

async function callAnthropicModel(
  budget: SubrequestBudget,
  model: string,
  messages: ChatMessage[],
  tools: McpTool[],
  onDelta: (text: string) => Promise<void>,
  baseUrlOverride?: string,
  signal?: AbortSignal,
  runId?: string,
  onReasoning?: (text: string) => Promise<void>,
): Promise<ModelTurnResult> {
  const { system, messages: anthropicMessages } = toAnthropicMessages(messages)
  const body: Record<string, unknown> = {
    model,
    max_tokens: anthropicMaxTokens(),
    stream: true,
    messages: anthropicMessages,
  }
  if (system) body.system = system
  if (tools.length) {
    body.tools = tools.map((tool) => ({
      name: tool.name,
      description: tool.description || '',
      input_schema: tool.inputSchema || { type: 'object' },
    }))
  }

  const requestStartedAt = Date.now()
  const response = await fetchWithBudget(budget, anthropicEndpoint(baseUrlOverride), {
    method: 'POST',
    headers: {
      'x-api-key': modelApiKey,
      'anthropic-version': '2023-06-01',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
    signal,
  }, 'essential')
  if (!response.ok) {
    const detail: any = await response.json().catch(() => null)
    const message = detail?.error?.message || detail?.message || `Model provider returned ${response.status}`
    if (response.status === 401 || response.status === 403) {
      throw new ApiError(response.status, message, 'MODEL_AUTH_FAILED')
    }
    throw new ApiError(response.status, message, 'MODEL_REQUEST_FAILED')
  }

  let content = ''
  // tool_use blocks stream their arguments as input_json_delta fragments, so
  // each accumulator holds the growing JSON string until its block stops.
  const calls = new Map<number, ToolCallAccumulator>()
  let activeToolIndex = -1
  let firstDeltaAt = 0
  for await (const data of sseDataLines(response, signal)) {
    if (!data) continue
    let chunk: any
    try {
      chunk = JSON.parse(data)
    } catch {
      continue
    }
    if (chunk?.type === 'content_block_start') {
      const block = chunk?.content_block
      if (block?.type === 'tool_use') {
        activeToolIndex = Number(chunk.index ?? 0)
        calls.set(activeToolIndex, {
          id: block.id || `tool_${activeToolIndex}`,
          name: block.name || '',
          arguments: '',
        })
      }
      continue
    }
    if (chunk?.type === 'content_block_delta') {
      const delta = chunk?.delta
      if (delta?.type === 'text_delta' && delta.text) {
        if (!firstDeltaAt) {
          firstDeltaAt = Date.now()
          console.log({
            event: 'run.stage',
            run_id: runId,
            stage: 'provider_first_delta',
            duration_ms: firstDeltaAt - requestStartedAt,
          })
        }
        content += delta.text
        await onDelta(delta.text)
      } else if (delta?.type === 'thinking_delta' && delta.thinking) {
        if (onReasoning) await onReasoning(delta.thinking)
      } else if (delta?.type === 'input_json_delta' && delta.partial_json) {
        const current = calls.get(activeToolIndex)
        if (current) current.arguments += delta.partial_json
      }
      continue
    }
    if (chunk?.type === 'content_block_stop') {
      activeToolIndex = -1
      continue
    }
    if (chunk?.type === 'message_stop') break
  }
  return {
    content,
    toolCalls: [...calls.values()].filter((call) => call.name),
  }
}

// ---------------------------------------------------------------------------
// pi StreamFn (the pi agent loop calls this per assistant response)
// ---------------------------------------------------------------------------

const EMPTY_USAGE: Usage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
}

export interface StreamFnConfig {
  /** Invocation budget every provider request is charged to. */
  budget: SubrequestBudget
  /** Per-agent base URL from the claim, resolved exactly as the builtin engine resolves it. */
  baseUrlOverride?: string
  protocol: ModelProtocol
  signal?: AbortSignal
  runId?: string
}

/**
 * Build the StreamFn for one turn. Stateless apart from the captured claim
 * config; the provider key comes from the module runtime env (Worker secret).
 */
export function createStreamFn(config: StreamFnConfig): StreamFn {
  return (model, context) => {
    const stream = createAssistantMessageEventStream()
    void streamModelResponse(model, context, config, stream)
    return stream
  }
}

async function streamModelResponse(
  model: Model<Api>,
  context: TranscriptContext,
  config: StreamFnConfig,
  stream: ReturnType<typeof createAssistantMessageEventStream>,
): Promise<void> {
  const partial: AssistantMessage = {
    role: 'assistant',
    content: [],
    api: model.api,
    provider: model.provider,
    model: model.id,
    usage: EMPTY_USAGE,
    stopReason: 'pending',
    timestamp: Date.now(),
  }
  try {
    // pi puts the system prompt and tool declarations in the transcript's
    // system messages (normalizeContext / declareToolChanges); the declared
    // tools are what the model may call this response.
    const tools = getCurrentTools(context.messages).map((tool) => ({
      name: tool.name,
      description: tool.description,
      inputSchema: isRecord(tool.parameters) ? tool.parameters : { type: 'object' as const },
    }))
    const messages = toChatMessages(context.messages)

    // Protocol: start always precedes partial updates.
    stream.push({ type: 'start', partial })

    let textIndex = -1
    // Reasoning arrives before the answer on thinking models, so both block
    // indexes are allocated on demand instead of assuming position 0.
    let thinkingIndex = -1
    const result = await callModel(config.budget, model.id, messages, tools, async (text) => {
      if (textIndex < 0) {
        partial.content.push({ type: 'text', text: '' } as TextContent)
        textIndex = partial.content.length - 1
        stream.push({ type: 'text_start', contentIndex: textIndex, partial })
      }
      const block = partial.content[textIndex] as TextContent
      block.text += text
      stream.push({ type: 'text_delta', contentIndex: textIndex, delta: text, partial })
    }, config.baseUrlOverride, config.protocol, config.signal, config.runId, async (text) => {
      if (thinkingIndex < 0) {
        partial.content.push({ type: 'thinking', thinking: '' } as ThinkingContent)
        thinkingIndex = partial.content.length - 1
        stream.push({ type: 'thinking_start', contentIndex: thinkingIndex, partial })
      }
      const block = partial.content[thinkingIndex] as ThinkingContent
      block.thinking += text
      stream.push({ type: 'thinking_delta', contentIndex: thinkingIndex, delta: text, partial })
    })
    if (thinkingIndex >= 0) {
      stream.push({
        type: 'thinking_end',
        contentIndex: thinkingIndex,
        content: (partial.content[thinkingIndex] as ThinkingContent).thinking,
        partial,
      })
    }
    if (textIndex >= 0) {
      stream.push({ type: 'text_end', contentIndex: textIndex, content: (partial.content[textIndex] as TextContent).text, partial })
    }

    // pi tool calls carry parsed argument objects; arguments that are not a
    // JSON object become argumentsError so the loop's validation fails the
    // call with a clear message the model can act on.
    result.toolCalls.forEach((call, index) => {
      const id = call.id || `call_${index}_${call.name}`
      let args: JsonObject = {}
      let argsError: string | undefined
      try {
        const parsed = JSON.parse(call.arguments || '{}')
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
          args = parsed
        } else {
          argsError = 'The model\'s arguments were not a JSON object'
        }
      } catch (error) {
        argsError = error instanceof Error ? error.message : 'The model\'s arguments were not valid JSON'
      }
      const block: ToolCall = {
        type: 'toolCall',
        id,
        name: call.name,
        arguments: args,
        ...(argsError ? { argumentsError: argsError } : {}),
      }
      stream.push({ type: 'toolcall_start', contentIndex: partial.content.length, partial })
      partial.content.push(block)
      stream.push({ type: 'toolcall_end', contentIndex: partial.content.length - 1, toolCall: block, partial })
    })

    const stopReason: StopReason = result.toolCalls.length ? 'toolUse' : 'stop'
    partial.stopReason = stopReason
    stream.push({ type: 'done', reason: stopReason === 'toolUse' ? 'toolUse' : 'stop', message: partial })
  } catch (error) {
    // StreamFn contract: never throw — encode the failure in the stream.
    const errorMessage = error instanceof Error ? error.message : String(error)
    partial.stopReason = 'error'
    partial.errorMessage = errorMessage
    if (error instanceof ApiError) {
      // Preserve status + error code for the engine to reconstruct, so runTurn
      // reports the same error_code the builtin engine would have.
      partial.diagnostics = [{
        type: 'model_request_failed',
        timestamp: Date.now(),
        error: { name: 'ApiError', message: errorMessage },
        details: { status: error.status, errorCode: error.errorCode },
      }]
    }
    stream.push({ type: 'error', reason: 'error', error: partial })
  }
}

/** Flatten a pi transcript back into the OpenAI-ish ChatMessage shape both adapters speak. */
function toChatMessages(messages: Message[]): ChatMessage[] {
  const out: ChatMessage[] = []
  for (const message of messages) {
    if (message.role === 'system') {
      out.push({ role: 'system', content: contentText(message.content) })
    } else if (message.role === 'user') {
      out.push({ role: 'user', content: contentText(message.content) })
    } else if (message.role === 'assistant') {
      const text = contentText(message.content)
      const toolCalls = message.content.filter((block): block is ToolCall => block.type === 'toolCall')
      if (toolCalls.length) {
        out.push({
          role: 'assistant',
          content: text,
          tool_calls: toolCalls.map((block) => ({
            id: block.id,
            type: 'function' as const,
            function: { name: block.name, arguments: JSON.stringify(block.arguments) },
          })),
        })
      } else if (text) {
        // Empty assistant turns are dropped: several providers reject them,
        // and the builtin engine's Anthropic adapter already skipped them.
        out.push({ role: 'assistant', content: text })
      }
    } else if (message.role === 'toolResult') {
      out.push({ role: 'tool', tool_call_id: message.toolCallId, content: contentText(message.content) })
    }
  }
  return out
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
