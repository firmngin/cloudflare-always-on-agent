/**
 * spawn_subagent contract: the model may split work off, but a run cannot fan out
 * without bound and the parent only receives a bounded summary.
 */
import assert from 'node:assert/strict'
import test from 'node:test'
import {
  createSpawnSubagentTool,
  SUBAGENT_MAX_CALLS,
  SUBAGENT_RESULT_LIMIT,
} from '../src/pi/subagent.ts'

function resultText(result: { content: unknown[] }): string {
  const [first] = result.content as { type: string, text?: string }[]
  return String(first?.text || '')
}

test('spawn_subagent runs the task and returns the summary', async () => {
  const tasks: string[] = []
  const [tool] = createSpawnSubagentTool({
    runTask: async (task) => {
      tasks.push(task)
      return `3 devices offline (${task})`
    },
  })

  const result = await tool.execute('call_1', { task: 'count offline devices' })
  assert.deepEqual(tasks, ['count offline devices'])
  assert.equal(resultText(result), '3 devices offline (count offline devices)')
})

test('spawn_subagent refuses more than the per-run limit', async () => {
  const [tool] = createSpawnSubagentTool({ runTask: async () => 'ok' })

  for (let index = 0; index < SUBAGENT_MAX_CALLS; index += 1) {
    await tool.execute(`call_${index}`, { task: `task ${index}` })
  }
  await assert.rejects(
    () => tool.execute('call_over', { task: 'one too many' }),
    /Sub-agent limit reached/,
  )
})

test('spawn_subagent rejects an empty task', async () => {
  const [tool] = createSpawnSubagentTool({ runTask: async () => 'ok' })
  await assert.rejects(() => tool.execute('call_1', { task: '   ' }), /must describe the work/)
  await assert.rejects(() => tool.execute('call_2', {}), /must describe the work/)
})

test('spawn_subagent truncates a long sub-agent answer', async () => {
  const [tool] = createSpawnSubagentTool({ runTask: async () => 'x'.repeat(SUBAGENT_RESULT_LIMIT + 50) })
  const text = resultText(await tool.execute('call_1', { task: 'long answer' }))
  assert.equal(text.includes('…[truncated]'), true)
  assert.equal(text.length < SUBAGENT_RESULT_LIMIT + 40, true)
})
