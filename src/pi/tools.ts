/**
 * MCP tools mapped into pi AgentTool[], plus the shared tool-output helpers
 * (safeJson / summarize, moved here from src/index.ts — the pi engine and the
 * builtin engine both use them).
 *
 * pi's Tool.parameters accepts ANY JSON Schema object (the vendored pi-schema
 * compiler validates plain JSON Schema directly, with coercion), so MCP tool
 * inputSchema maps through as-is — no TypeBox conversion needed. Argument
 * validation/coercion happens in the vendored loop's validateToolArguments
 * before execute() runs; thrown errors become isError tool results the model
 * can react to.
 *
 * The McpClient itself stays in src/index.ts (existing MCP-over-HTTP client);
 * this module only wraps it, so it imports the client as a type only.
 */

import type { AgentTool, AgentToolResult } from '../vendor/pi-agent/types'
import type { McpClient } from '../index'

export interface McpTool {
  name: string
  description?: string
  inputSchema?: Record<string, unknown>
}

/** Parse a JSON argument string; non-object or invalid JSON becomes {} (same semantics as before). */
export function safeJson(value: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(value || '{}')
    return parsed && typeof parsed === 'object' ? parsed : {}
  } catch {
    return {}
  }
}

/** One-line capped preview of a tool output for the tool.result event payload. */
export function summarize(value: string): string {
  const text = String(value || '').replace(/\s+/g, ' ').trim()
  return text.length > 240 ? `${text.slice(0, 240)}…` : text
}

/**
 * Map every MCP tool the client listed into a pi AgentTool. execute() resolves
 * through mcp.call: thrown MCP errors (including isError results) propagate to
 * the vendored loop, which turns them into isError tool results — the same
 * failure path the builtin engine takes.
 */
export function mapMcpTools(client: McpClient): AgentTool[] {
  return client.tools.map((tool): AgentTool => {
    const name = tool.name
    return {
      name,
      label: name,
      description: tool.description || '',
      parameters: isRecord(tool.inputSchema) ? tool.inputSchema : { type: 'object' },
      async execute(_toolCallId: string, params: unknown): Promise<AgentToolResult<any>> {
        const output = await client.call(name, isRecord(params) ? params : {})
        return {
          content: [{ type: 'text', text: output }],
          details: {},
        }
      },
    }
  })
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
