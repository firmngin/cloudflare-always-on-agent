/**
 * spawn_subagent: the model decides when a piece of work is better split off.
 *
 * Depth is one — the sub-agent gets the parent's tools (the MCP scopes are
 * read-only, so the clone is read-only too) but NOT this tool, so it cannot
 * spawn a third level. Each spawn costs real model calls, so a run may spawn
 * `maxCalls` times and each sub-agent may run `maxTurns` assistant turns; both
 * spend from the same invocation subrequest budget as the parent.
 *
 * The nested loop is injected (`runTask`) so the tool contract — limits,
 * validation, truncation — is testable without a provider.
 */

import {
  runAgentLoop,
  type AgentLoopConfig,
  type AgentTool,
  type AgentToolResult,
} from '@earendil-works/pi-agent-core'
import { contentText, type Api, type AssistantMessage, type Message, type Model } from '@earendil-works/pi-ai'
import { createStreamFn, type ModelProtocol } from './stream-fn'
import type { SubrequestBudget } from '../runtime/subrequest-budget'

/** One spawn may run this many assistant turns before the loop is cut. */
export const SUBAGENT_MAX_TURNS = 3
/** Spawns allowed per run (each one is a whole nested conversation). */
export const SUBAGENT_MAX_CALLS = 2
/** The parent gets a summary, not the sub-agent's transcript. */
export const SUBAGENT_RESULT_LIMIT = 4000

export type SubagentRunner = (task: string) => Promise<string>

export interface SpawnSubagentOptions {
  runTask: SubagentRunner
  maxCalls?: number
}

export interface SubagentRunnerOptions {
  /** Parent system prompt: the sub-agent inherits the same platform + owner rules. */
  systemPrompt: string
  /** Parent tools only — never the spawn tool itself. */
  tools: AgentTool[]
  model: Model<Api>
  protocol: ModelProtocol
  budget: SubrequestBudget
  runId?: string
  baseUrlOverride?: string
  signal?: AbortSignal
  maxTurns?: number
}

const SPAWN_PARAMETERS = Object.freeze({
  type: 'object',
  properties: {
    task: {
      type: 'string',
      description:
        'A self-contained instruction for the sub-agent: what to find out, over which device/entity/time range, and what to report back. The sub-agent does not see this conversation.',
    },
  },
  required: ['task'],
  additionalProperties: false,
})

export function createSpawnSubagentTool(options: SpawnSubagentOptions): AgentTool[] {
  const maxCalls = Math.max(1, Math.floor(Number(options.maxCalls ?? SUBAGENT_MAX_CALLS) || SUBAGENT_MAX_CALLS))
  let spawned = 0

  const tool: AgentTool = {
    name: 'spawn_subagent',
    label: 'spawn_subagent',
    description:
      `Delegate a self-contained read-only task to a sub-agent (max ${maxCalls} per run, ${SUBAGENT_MAX_TURNS} turns each). ` +
      'Use it when independent lookups can run separately, then combine the answers yourself.',
    parameters: SPAWN_PARAMETERS,
    async execute(_toolCallId: string, params: unknown): Promise<AgentToolResult<unknown>> {
      const task = isRecord(params) && typeof params.task === 'string' ? params.task.trim() : ''
      if (!task) {
        throw new Error('The "task" argument must describe the work the sub-agent should do')
      }
      if (spawned >= maxCalls) {
        throw new Error(`Sub-agent limit reached (${maxCalls} per run). Do the remaining work yourself.`)
      }
      spawned += 1
      const result = await options.runTask(task)
      return {
        content: [{ type: 'text', text: truncate(result || 'The sub-agent returned no answer.', SUBAGENT_RESULT_LIMIT) }],
        details: {},
      }
    },
  }
  return [tool]
}

/** The real nested loop: same model, tools and budget, one depth. */
export function createSubagentRunner(options: SubagentRunnerOptions): SubagentRunner {
  const maxTurns = Math.max(1, Math.floor(Number(options.maxTurns ?? SUBAGENT_MAX_TURNS) || SUBAGENT_MAX_TURNS))

  return async (task: string) => {
    let finalText = ''
    let turns = 0
    let last: AssistantMessage | undefined

    const config: AgentLoopConfig = {
      model: options.model,
      convertToLlm: (messages: Message[]) =>
        messages.filter(
          (message) =>
            message.role === 'system' ||
            message.role === 'user' ||
            message.role === 'assistant' ||
            message.role === 'toolResult',
        ),
      toolExecution: 'sequential',
      getSteeringMessages: async () => [],
      getFollowUpMessages: async () => [],
      finishTurn: (turn) => {
        turns += 1
        const message = turn.message as AssistantMessage
        last = message
        const usedTools = message.content.some((block) => block.type === 'toolCall')
        if (!usedTools) {
          const text = contentText(message.content)
          if (text) finalText = text
        }
        return turns >= maxTurns ? { action: 'end' } : undefined
      },
    }

    await runAgentLoop(
      [
        { role: 'system', content: subagentPrompt(options.systemPrompt), timestamp: Date.now() },
        { role: 'user', content: task, timestamp: Date.now() },
      ],
      { messages: [], tools: options.tools },
      config,
      // Inner turns are not streamed: the parent's run shows one tool call and
      // one summary, which is what the reader needs.
      async () => {},
      options.signal,
      createStreamFn({
        budget: options.budget,
        baseUrlOverride: options.baseUrlOverride,
        protocol: options.protocol,
        signal: options.signal,
        runId: options.runId,
      }),
    )

    if (last && (last.stopReason === 'error' || last.stopReason === 'aborted')) {
      throw new Error(`The sub-agent stopped before answering (${last.stopReason}).`)
    }
    return finalText.trim()
  }
}

function subagentPrompt(parentSystemPrompt: string): string {
  const rules = [
    '# Sub-agent',
    'You are a sub-agent working on one delegated task. Finish it and reply with the result — never hand work back or ask what to do next.',
    'Your tools are read-only: report what you find, never change device state.',
  ].join('\n')
  return parentSystemPrompt ? `${parentSystemPrompt}\n\n${rules}` : rules
}

function truncate(text: string, limit: number): string {
  const value = String(text || '')
  return value.length > limit ? `${value.slice(0, limit)}\n…[truncated]` : value
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
