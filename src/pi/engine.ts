/**
 * pi-core turn engine.
 *
 * executeTurnPi() mirrors the builtin executeTurn signature/behavior
 * (agentId, claim, events -> final content + whether MAX_STEPS ended it) but
 * runs pi's agent loop
 * (runAgentLoop from @earendil-works/pi-agent-core) with:
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

import {
  runAgentLoop,
  type AgentEventSink,
  type AgentLoopConfig,
  type AgentTool,
  type AgentToolResult,
} from '@earendil-works/pi-agent-core'
import {
  contentText,
  type Api,
  type AssistantMessage,
  type Message,
  type Model,
  type SystemMessage,
  type TextContent,
  type UserMessage,
} from '@earendil-works/pi-ai'
import type { Claim, EventStream, McpClient } from '../index'
import { shouldConnectMcp, type TurnOutput } from '../runtime/turn'
import type { SubrequestBudget } from '../runtime/subrequest-budget'
import { ApiError, createStreamFn, resolveModelProtocol, type ModelProtocol } from './stream-fn'
import { mapMcpTools, summarize } from './tools'
import { createLoadSkillTool, parseSkills, renderSkillsIndex } from './skills'
import { createSpawnSubagentTool, createSubagentRunner } from './subagent'

// Reasoning text is forwarded for the dashboard timeline, but it never earns a
// bigger event payload than this.
const THINKING_TEXT_LIMIT = 8000

export interface PiEngineDeps {
  /** The template's MCP token exchange + client connect (src/index.ts). */
  connectMcp(agentId: string, signal?: AbortSignal): Promise<McpClient | null>
  /** MAX_STEPS, resolved by the caller with the same expression the builtin engine uses. */
  maxSteps: number
  /** The invocation's subrequest budget: every provider request is charged to it. */
  budget: SubrequestBudget
}

export async function executeTurnPi(agentId: string, claim: Claim, events: EventStream, deps: PiEngineDeps, signal?: AbortSignal): Promise<TurnOutput> {
  const modelId = claim.model
  if (!modelId) throw new ApiError(0, 'Agent config has no model', 'CONFIG_INVALID')
  const protocol = resolveModelProtocol(claim)
  const mcpStartedAt = Date.now()
  const mcpEnabled = shouldConnectMcp(claim)
  const mcp = mcpEnabled ? await deps.connectMcp(agentId, signal) : null
  console.log({
    event: 'run.stage',
    run_id: claim.run_id,
    stage: 'mcp_preflight',
    duration_ms: Date.now() - mcpStartedAt,
    enabled: mcpEnabled,
  })

  const skills = parseSkills(claim.skills)
  const skillsIndex = renderSkillsIndex(skills)
  // Two-level system prompt: platform capabilities first, then the owner's
  // instruction last (recency + ownership). Without platform_prompt (older
  // backends) the assembly is unchanged: task_prompt + skills index.
  const platformText = String(claim.platform_prompt || '').trim()
  const taskText = String(claim.task_prompt || '').trim()
  const sections: string[] = []
  if (platformText) sections.push(platformText)
  if (skillsIndex) sections.push(skillsIndex)
  if (taskText) sections.push(platformText ? `# Owner instructions\n\n${taskText}` : taskText)
  const systemPrompt = sections.join('\n\n')
  // The sub-agent sees the parent's tools but not `spawn_subagent`, so delegation
  // is one level deep (and read-only, because the MCP scopes are).
  const baseTools: AgentTool[] = [...(mcp ? mapMcpTools(mcp) : []), ...createLoadSkillTool(skills)]
  const model = buildPiModel(claim, protocol)
  const tools: AgentTool[] = [
    ...baseTools,
    ...createSpawnSubagentTool({
      runTask: createSubagentRunner({
        systemPrompt,
        tools: baseTools,
        model,
        protocol,
        budget: deps.budget,
        runId: claim.run_id,
        baseUrlOverride: claim.model_base_url,
        signal,
      }),
    }),
  ]

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
  let toolLoopHit = false
  let finalContent = ''
  let finalAssistant: AssistantMessage | undefined

  const config: AgentLoopConfig = {
    model,
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
      const usedTools = message.content.some((block) => block.type === 'toolCall')
      // Same final-content rule as the builtin step loop: a turn that made
      // tool calls resets the final output to ''; a tool-less turn's text
      // becomes it.
      if (usedTools) {
        finalContent = ''
      } else {
        const text = contentText(message.content)
        if (text) finalContent = text
      }
      if (assistantTurns >= maxSteps) {
        // The guard only ended this turn when the loop still wanted another
        // one (a tool call); a tool-less final turn ends it either way.
        toolLoopHit = usedTools
        return { action: 'end' }
      }
    },
  }

  await runAgentLoop(
    [promptMessage, userMessage],
    { messages: history, tools },
    config,
    mapAgentEvents(events, signal),
    signal,
    createStreamFn({
      budget: deps.budget,
      baseUrlOverride: claim.model_base_url,
      protocol,
      signal,
      runId: claim.run_id,
    }),
  )

  if (finalAssistant && (finalAssistant.stopReason === 'error' || finalAssistant.stopReason === 'aborted')) {
    throw turnFailure(finalAssistant)
  }
  await events.flush('bestEffort', signal)
  return { content: finalContent, toolLoopHit }
}

/** pi AgentEvent -> runtime EventStream events, with unchanged payload shapes and names. */
function mapAgentEvents(events: EventStream, signal?: AbortSignal): AgentEventSink {
  // One reasoning block per model call: the dashboard places it on the timeline
  // by the offset recorded when it started, so the text travels with the stop.
  let thinkingStartedAt = 0
  let thinkingText = ''
  return async (event) => {
    if (event.type === 'message_update') {
      const update = event.assistantMessageEvent
      if (update.type === 'text_delta') {
        await events.pushDelta(update.delta, signal)
        return
      }
      if (update.type === 'thinking_start') {
        thinkingStartedAt = Date.now()
        thinkingText = ''
        await events.pushAndFlush('thinking.start', '', signal)
        return
      }
      if (update.type === 'thinking_delta') {
        if (thinkingText.length < THINKING_TEXT_LIMIT) thinkingText += update.delta
        return
      }
      if (update.type === 'thinking_end') {
        const duration = thinkingStartedAt > 0 ? Date.now() - thinkingStartedAt : 0
        thinkingStartedAt = 0
        await events.pushAndFlush(
          'thinking.stop',
          JSON.stringify({ duration_ms: duration, content: thinkingText.slice(0, THINKING_TEXT_LIMIT) }),
          signal,
        )
        thinkingText = ''
      }
      return
    }
    if (event.type === 'tool_execution_start') {
      // args are already parsed argument objects (pi normalizes them).
      await events.pushAndFlush('tool.call', JSON.stringify({ name: event.toolName, arguments: event.args }), signal)
      return
    }
    if (event.type === 'tool_execution_end') {
      const text = toolResultText(event.result)
      await events.pushAndFlush(
        'tool.result',
        event.isError
          ? JSON.stringify({ name: event.toolName, error: text })
          : JSON.stringify({ name: event.toolName, summary: summarize(text) }),
        signal,
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
