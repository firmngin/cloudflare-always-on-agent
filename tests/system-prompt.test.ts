/**
 * Prompt assembly contract: identity first, platform rules second, skills index
 * third, the owner's instructions last (that order is what makes the agent know
 * who it is and what it is for).
 */
import assert from 'node:assert/strict'
import test from 'node:test'
import { composeSystemPrompt } from '../src/pi/prompt.ts'

test('identity, platform and owner instructions stay in order', () => {
  const prompt = composeSystemPrompt({
    agentName: 'Device Ops',
    platformText: 'PLATFORM',
    skillsIndex: 'SKILLS',
    taskText: 'TASK',
  })
  const order = ['You are "Device Ops"', 'PLATFORM', 'SKILLS', '# Owner instructions', 'TASK']
  let cursor = -1
  for (const part of order) {
    const at = prompt.indexOf(part)
    assert.ok(at > cursor, `${part} out of order in:\n${prompt}`)
    cursor = at
  }
})

test('an older backend without agent_name keeps the old prompt', () => {
  assert.equal(
    composeSystemPrompt({ platformText: 'PLATFORM', taskText: 'TASK' }),
    'PLATFORM\n\n# Owner instructions\n\nTASK',
  )
  assert.equal(composeSystemPrompt({ taskText: 'TASK' }), 'TASK')
  assert.equal(composeSystemPrompt({}), '')
})

test('a name alone is a valid prompt', () => {
  assert.equal(
    composeSystemPrompt({ agentName: '  Device Ops  ' }),
    'You are "Device Ops", an Always-On agent in this workspace.',
  )
})
