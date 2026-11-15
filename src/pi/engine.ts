/**
 * pi-core turn engine.
 *
 * executeTurnPi() mirrors the builtin executeTurn signature/behavior
 * (agentId, claim, events -> final content string) but runs the vendored
 * pi agent loop (src/vendor/pi-agent/agent-loop.ts) with:
 *   - context   : claim history (user/assistant turns) + the task prompt
 *                 (with the skills index appended) + the user message
 *   - tools     : MCP tools mapped to pi AgentTool[] + the load_skill builtin
 *   - model     : a pi Model built from the claim (id/provider/protocol)
 *   - StreamFn  : the template adapter in ./stream-fn.ts (BYOK provider key)
 *   - toolExecution: "sequential" (MCP over HTTP)
 *   - no steering/queue/follow-up hooks (always [])
 *   - max steps enforced through finishTurn { action: "end" } after
 *     MAX_STEPS assistant turns, matching the builtin step loop
 *
 * Event contract (UNCHANGED payload shapes/names):
 *   assistant text delta  -> 'delta'        (raw text chunk, batched flush)
 *   tool call             -> 'tool.call'    JSON {name, arguments}
 *   tool result           -> 'tool.result'  JSON {name, summary} | {name, error}
 *   aborts/model errors   -> thrown so runTurn emits 'error' + finishRun failed
 * Final content: the last assistant turn's text when it made no tool calls,
 * otherwise '' — identical to the builtin engine's step loop.
 */

import { runAgentLoop, type AgentEventSink } from '../vendor/pi-agent/agent-loop'
import type { AgentLoopConfig, AgentTool, AgentToolResult } from '../vendor/pi-agent/types'
import {
  contentText,
  type Api,
  type AssistantMessage,
  type Message,
  type Model,
  type SystemMessage,
  type TextContent,
  type UserMessage,
} from '../vendor/pi-ai/index'
import type { Claim, EventStream, McpClient } from '../index'
import { ApiError, createStreamFn, resolveModelProtocol, type ModelProtocol } from './stream-fn'
import { mapMcpTools, summarize } from './tools'
import { createLoadSkillTool, parseSkills, renderSkillsIndex } from './skills'

export interface PiEngineDeps {
  /** The template's MCP token exchange + client connect (src/index.ts). */
  connectMcp(agentId: string): Promise<McpClient | null>
  /** MAX_STEPS, resolved by the caller with the same expression the builtin engine uses. */
  maxSteps: number
}

export async function executeTurnPi(agentId: string, claim: Claim, events: EventStream, deps: PiEngineDeps): Promise<string> {
  const model = claim.model
  if (!model) throw new ApiError(0, 'Agent config has no model', 'CONFIG_INVALID')
  const protocol = resolveModelProtocol(claim)
  const mcp = await deps.connectMcp(agentId)

  const skills = parseSkills(claim.skills)
  const skillsIndex = renderSkillsIndex(skills)
  const systemPrompt = skillsIndex ? `${claim.task_prompt}\n\n${skillsIndex}` : claim.task_prompt
  const tools: AgentTool[] = [...(mcp ? mapMcpTools(mcp) : []), ...createLoadSkillTool(skills)]

  // pi messages carry timestamps; history roles are filtered exactly like the
  // builtin engine's history filter (user/assistant only). Assistant turns are
  // reconstructed as full AssistantMessage records (pi's Message union has no
  // bare {role, content} assistant shape); zeroed usage + stopReason 'stop'
  // keep them valid while the provider adapters only serialize their text.
  const now = Date.now()
  const history: Message[] = (claim.history || [])
    .filter((item) => item.role === 'user' || item.role === 'assistant')
    .map((item): Message =>
      item.role === 'user'
        ? { role: 'user', content: item.content || '', timestamp: now }
        : {
            role: 'assistant',
            content: [{ type: 'text', text: item.content || '' }],
            api: protocol === 'anthropic' ? 'anthropic-messages' : 'openai-completions',
            provider: String(claim.provider || '') || 'custom',
            model: claim.model,
            usage: {
              input: 0,
              output: 0,
              cacheRead: 0,
              cacheWrite: 0,
              totalTokens: 0,
              cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
            },
            stopReason: 'stop',
            timestamp: now,
          },
    )
  const promptMessage: SystemMessage = { role: 'system', content: systemPrompt, timestamp: Date.now() }
  const userMessage: UserMessage = { role: 'user', content: claim.message?.content || '', timestamp: Date.now() }

  // The loop has no turn-limit option; MAX_STEPS is enforced via finishTurn.
  // Non-finite/invalid values fail closed to a single bounded turn instead of
  // an unbounded loop (the builtin's NaN case runs zero steps).
  const maxSteps = Number.isFinite(deps.maxSteps) && deps.maxSteps >= 1 ? Math.floor(deps.maxSteps) : 1
  let assistantTurns = 0
  let finalContent = ''
  let finalAssistant: AssistantMessage | undefined

  const config: AgentLoopConfig = {
    model: buildPiModel(claim, protocol),
    // Transcript messages are already LLM-compatible pi Messages; pass the
    // standard roles through unchanged.
    convertToLlm: (messages) =>
      messages.filter(
        (message) =>
          message.role === 'system' || message.role === 'user' || message.role === 'assistant' || message.role === 'toolResult',
      ),
    toolExecution: 'sequential',
    getSteeringMessages: async () => [],
    getFollowUpMessages: async () => [],
    finishTurn: (turn) => {
      assistantTurns++
      const message = turn.message as AssistantMessage
      finalAssistant = message
      // Same final-content rule as the builtin step loop: a turn that made
      // tool calls resets the final output to ''; a tool-less turn's text
      // becomes it.
      if (message.content.some((block) => block.type === 'toolCall')) {
        finalContent = ''
      } else {
        const text = contentText(message.content)
        if (text) finalContent = text
      }
      if (assistantTurns >= maxSteps) return { action: 'end' }
    },
  }

  await runAgentLoop(
    [promptMessage, userMessage],
    { messages: history, tools },
    config,
    mapAgentEvents(events),
    undefined,
    createStreamFn({ baseUrlOverride: claim.model_base_url, protocol }),
  )

  if (finalAssistant && (finalAssistant.stopReason === 'error' || finalAssistant.stopReason === 'aborted')) {
    throw turnFailure(finalAssistant)
  }
  await events.flush()
  return finalContent
}

/** pi AgentEvent -> runtime EventStream events, with unchanged payload shapes and names. */
function mapAgentEvents(events: EventStream): AgentEventSink {
  return async (event) => {
    if (event.type === 'message_update' && event.assistantMessageEvent.type === 'text_delta') {
      events.push('delta', event.assistantMessageEvent.delta)
      await events.flushIfLarge()
      return
    }
    if (event.type === 'tool_execution_start') {
      // args are already parsed argument objects (pi normalizes them).
      await events.pushAndFlush('tool.call', JSON.stringify({ name: event.toolName, arguments: event.args }))
      return
    }
    if (event.type === 'tool_execution_end') {
      const text = toolResultText(event.result)
      await events.pushAndFlush(
        'tool.result',
        event.isError
          ? JSON.stringify({ name: event.toolName, error: text })
          : JSON.stringify({ name: event.toolName, summary: summarize(text) }),
      )
    }
  }
}

function toolResultText(result: unknown): string {
  const content = (result as AgentToolResult<any> | undefined)?.content
  return Array.isArray(content)
    ? content.filter((block): block is TextContent => block?.type === 'text').map((block) => block.text).join('\n')
    : ''
}

/**
 * Rebuild the error the builtin engine would have thrown. The StreamFn cannot
 * throw (pi contract), so it stashes ApiError status + code on the assistant
 * message as a diagnostic; plain failures (e.g. network errors) come back as
 * generic Errors -> runTurn reports WORKER_ERROR, same as today.
 */
function turnFailure(message: AssistantMessage): Error {
  const text = message.errorMessage || 'Model provider request failed'
  const diagnostic = message.diagnostics?.find((item) => item.type === 'model_request_failed')
  const rawDetails = diagnostic?.details
  const details = isRecord(rawDetails) ? rawDetails : undefined
  const status = typeof details?.status === 'number' ? details.status : undefined
  const errorCode = typeof details?.errorCode === 'string' ? details.errorCode : undefined
  if (status !== undefined) {
    return new ApiError(status, text, errorCode || 'MODEL_REQUEST_FAILED')
  }
  return new Error(text)
}

/** A pi Model built from the claim; cost/context fields are unknown to the Worker, so they are zeroed. */
function buildPiModel(claim: Claim, protocol: ModelProtocol): Model<Api> {
  return {
    id: claim.model,
    name: claim.model,
    api: protocol === 'anthropic' ? 'anthropic-messages' : 'openai-completions',
    provider: String(claim.provider || '') || 'custom',
    baseUrl: String(claim.model_base_url || ''),
    input: ['text'],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    reasoning: false,
    contextWindow: 0,
    maxTokens: 0,
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
