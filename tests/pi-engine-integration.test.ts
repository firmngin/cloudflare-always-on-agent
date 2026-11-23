/**
 * Phase 1 vertical slice: the official pi agent core runs a real Worker turn
 * through the unchanged runtime seams.
 *
 * Everything is local: the Firmngin runtime API (claim, MCP token, run events,
 * terminal PATCH), the MCP server and the model provider are mocked on
 * `globalThis.fetch`, so no provider key and no live backend are involved.
 */
import assert from 'node:assert/strict'
import test from 'node:test'
import {
  completionText,
  completionToolCall,
  installMockBackend,
  invocationSubrequests,
  MODEL_API_KEY,
  runScheduled,
} from './support/worker-harness.ts'

const TOOL_OUTPUT = 'living-room-sensor (online), garage-door (offline)'
const ANSWER = 'Two devices: living-room-sensor is online, garage-door is offline.'
const CHATTER = 'Checking the device list first.'

/** Assistant turn 1: a text delta, then the approved read-only Firmngin tool call. */
const TOOL_TURN = completionToolCall('call_list_devices', 'list_devices', { limit: 5 }, CHATTER)
/** Assistant turn 2: the final answer after the tool result. */
const FINAL_TURN = completionText(ANSWER)

test('pi turn streams deltas, runs the read-only tool once, and reports an accepted final', async () => {
  const mock = installMockBackend({ completions: [TOOL_TURN, FINAL_TURN], toolOutput: TOOL_OUTPUT })
  try {
    await runScheduled(mock.env)

    // Claim -> terminal report: the backend acknowledged the run exactly once.
    assert.equal(mock.patches.length, 1, 'one terminal run report')
    assert.equal(mock.patches[0].status, 200)
    assert.deepEqual(mock.patches[0].body, { status: 'succeeded', content: ANSWER, output: ANSWER, tool_loop_hit: false })

    // The tool ran exactly once: no double execution across the official loop.
    assert.deepEqual(mock.toolCalls, ['list_devices'])
    assert.equal(mock.completions.length, 2, 'one model request per assistant turn')

    // Event stream order and payloads are unchanged: start, delta, tool.call,
    // tool.result, final.
    assert.deepEqual(
      mock.events.filter((event) => event.type !== 'delta').map((event) => event.type),
      ['message.start', 'tool.call', 'tool.result', 'message.final'],
    )
    assert.deepEqual(
      mock.events.filter((event) => event.type === 'delta').map((event) => event.content),
      [CHATTER, ANSWER],
      'first delta is the streamed tool turn, the answer streams after the tool',
    )
    assert.deepEqual(
      JSON.parse(mock.events.find((event) => event.type === 'tool.call')!.content),
      { name: 'list_devices', arguments: { limit: 5 } },
    )
    assert.deepEqual(
      JSON.parse(mock.events.find((event) => event.type === 'tool.result')!.content),
      { name: 'list_devices', summary: TOOL_OUTPUT },
    )
    assert.equal(mock.events.find((event) => event.type === 'message.final')!.content, ANSWER)

    // Budget: one invocation with one turn stays inside a single turn's
    // subrequests, and the report is not retried when it was accepted.
    const spent = invocationSubrequests(mock.logs)
    assert.equal(typeof spent, 'number')
    assert.ok(spent! <= 25, `expected a single-turn subrequest count, got ${spent}`)
    assert.equal(mock.requests.filter((request) => request.method === 'PATCH').length, 1)
  } finally {
    mock.restore()
  }
})

test('an invalid model key fails the run with the builtin engine error code', async () => {
  const codes: string[] = []
  const messages: string[] = []
  for (const engine of ['pi', 'builtin'] as const) {
    const mock = installMockBackend({ engine, modelStatus: 401 })
    try {
      await runScheduled(mock.env)
      assert.equal(mock.patches.length, 1)
      const report = mock.patches[0].body as Record<string, string>
      assert.equal(report.status, 'failed')
      codes.push(String(report.error_code))
      messages.push(String(report.error_message))
      assert.ok(
        mock.events.some((event) => event.type === 'error'),
        'the failure is streamed as an error event',
      )
      assert.ok(!mock.events.some((event) => event.type === 'message.final'))
      // The public message stays the provider's text, never the Worker secret.
      assert.ok(!String(report.error_message).includes(MODEL_API_KEY))
      assert.equal(mock.toolCalls.length, 0, 'no tool runs after an auth failure')
    } finally {
      mock.restore()
    }
  }
  assert.deepEqual(codes, ['MODEL_AUTH_FAILED', 'MODEL_AUTH_FAILED'])
  assert.equal(messages[0], messages[1], 'same public message as the builtin engine')
})

test('the builtin engine returns the same answer and events for the same claim', async () => {
  const runs: Array<{ report: Record<string, unknown>, events: string[], patches: number }> = []
  for (const engine of ['pi', 'builtin'] as const) {
    const mock = installMockBackend({ engine, completions: [TOOL_TURN, FINAL_TURN], toolOutput: TOOL_OUTPUT })
    try {
      await runScheduled(mock.env)
      runs.push({
        report: mock.patches[0].body,
        events: mock.events.filter((event) => event.type !== 'delta').map((event) => event.type),
        patches: mock.requests.filter((request) => request.method === 'PATCH').length,
      })
      assert.deepEqual(mock.toolCalls, ['list_devices'])
    } finally {
      mock.restore()
    }
  }
  assert.deepEqual(runs[0], runs[1], 'builtin remains a like-for-like rollback')
  assert.deepEqual(runs[0].report, { status: 'succeeded', content: ANSWER, output: ANSWER, tool_loop_hit: false })
})

test('a turn the step guard ends reports tool_loop_hit', async () => {
  // MAX_STEPS is 8 in the harness and every allowed turn still asks for a tool,
  // so only the guard can end the loop.
  const completions = Array.from({ length: 8 }, (_, index) =>
    completionToolCall(`call_guard_${index}`, 'list_devices', { limit: 5 }))
  for (const engine of ['pi', 'builtin'] as const) {
    const mock = installMockBackend({ engine, completions })
    try {
      await runScheduled(mock.env)
      assert.equal(mock.patches.length, 1)
      assert.equal(mock.patches[0].body.tool_loop_hit, true, `${engine} engine reports the guard`)
      assert.equal(mock.toolCalls.length, 8, 'every allowed step ran')
    } finally {
      mock.restore()
    }
  }
})
