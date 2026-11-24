/**
 * System-prompt assembly, shared by both engines (the builtin step loop in
 * src/index.ts and the pi loop in src/pi/engine.ts).
 *
 * Order is deliberate: identity, then the platform rules Firmngin owns, then the
 * skills index, then the owner's instructions. The owner's task prompt always
 * comes last (recency + ownership); platform rules are standing constraints the
 * owner may specialize but never override.
 *
 * `agent_name` is optional so an older backend keeps working: without it the
 * prompt is exactly what it was before.
 */

export interface SystemPromptInput {
  agentName?: string
  platformText?: string
  skillsIndex?: string
  taskText?: string
}

export function composeSystemPrompt(input: SystemPromptInput): string {
  const name = String(input.agentName || '').trim()
  const platform = String(input.platformText || '').trim()
  const skills = String(input.skillsIndex || '').trim()
  const task = String(input.taskText || '').trim()

  const sections: string[] = []
  // Quoted so the model treats it as a label, not as an instruction of its own.
  if (name) sections.push(`You are "${name}", an Always-On agent in this workspace.`)
  if (platform) sections.push(platform)
  if (skills) sections.push(skills)
  if (task) sections.push(platform ? `# Owner instructions\n\n${task}` : task)
  return sections.join('\n\n')
}
