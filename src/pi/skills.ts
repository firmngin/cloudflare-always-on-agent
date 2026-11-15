/**
 * pi-style skills (agentskills.io pattern) for a filesystemless Worker.
 *
 * A claim may carry `skills`: a list of skill definitions (name + description
 * + full content, or a raw SKILL.md markdown body). The engine adds a skills
 * INDEX to the system prompt and registers a builtin `load_skill(name)` tool;
 * the full skill content is only returned on demand (progressive disclosure),
 * exactly like pi's skills.
 *
 * The field is optional on Claim: absent or empty -> the feature is inert and
 * behavior is identical to a turn without skills. Invalid entries are warned
 * about and skipped — never thrown.
 */

import type { AgentTool, AgentToolResult } from '../vendor/pi-agent/types'

/** A skill entry on the claim (all fields optional so old backends keep working). */
export interface SkillClaim {
  name?: string
  description?: string
  content?: string
  markdown?: string
}

export interface SkillDefinition {
  name: string
  description: string
  body: string
}

const LOAD_SKILL_PARAMETERS = Object.freeze({
  type: 'object',
  properties: {
    name: { type: 'string', description: "The skill's name, as listed in the skills index." },
  },
  required: ['name'],
  additionalProperties: false,
})

/** Parse + validate claim skills. Lenient: warn and skip anything unusable. */
export function parseSkills(raw: unknown): SkillDefinition[] {
  if (!Array.isArray(raw)) return []
  const skills: SkillDefinition[] = []
  const seen = new Set<string>()
  raw.forEach((entry, index) => {
    try {
      const skill = parseSkillEntry(entry, index)
      if (!skill) return
      if (seen.has(skill.name)) {
        console.warn(`skill "${skill.name}" (entry ${index}) is declared more than once; keeping the first`)
        return
      }
      seen.add(skill.name)
      skills.push(skill)
    } catch (error) {
      console.warn(`skill entry ${index} skipped (parse failed):`, error instanceof Error ? error.message : error)
    }
  })
  return skills
}

function parseSkillEntry(entry: unknown, index: number): SkillDefinition | null {
  if (!isRecord(entry)) {
    console.warn(`skill entry ${index} skipped: not an object`)
    return null
  }
  // A raw SKILL.md body may carry its own frontmatter; parse it leniently and
  // let explicit claim fields win.
  const markdown = typeof entry.markdown === 'string' ? entry.markdown : ''
  const frontmatter = markdown ? parseFrontmatter(markdown) : { fields: {} as Record<string, string>, body: '' }
  const name = firstString(entry.name, frontmatter.fields.name)
  if (!name) {
    console.warn(`skill entry ${index} skipped: no name (neither claim field nor frontmatter)`)
    return null
  }
  const description = firstString(entry.description, frontmatter.fields.description)
  const body = firstString(entry.content) || frontmatter.body
  if (!body) {
    console.warn(`skill "${name}" skipped: no content and no markdown body`)
    return null
  }
  return { name: name.trim(), description: description || '', body }
}

/** The skills INDEX for the system prompt: name + description + the load_skill nudge. Empty when there are none. */
export function renderSkillsIndex(skills: SkillDefinition[]): string {
  if (!skills.length) return ''
  const lines = skills.map((skill) => `- ${skill.name}${skill.description ? `: ${skill.description}` : ''}`)
  return [
    '## Skills',
    '',
    'The following skills extend your capabilities. Before working in one of these areas, call the load_skill tool with the skill\'s name to read its full instructions.',
    '',
    ...lines,
  ].join('\n')
}

/** The builtin load_skill tool; empty array when there are no skills (feature inert). */
export function createLoadSkillTool(skills: SkillDefinition[]): AgentTool[] {
  if (!skills.length) return []
  const byName = new Map(skills.map((skill) => [skill.name, skill]))
  const tool: AgentTool = {
    name: 'load_skill',
    label: 'load_skill',
    description:
      'Load the full instructions of a skill listed in the skills index. Call this before working in the skill\'s area.',
    parameters: LOAD_SKILL_PARAMETERS,
    async execute(_toolCallId: string, params: unknown): Promise<AgentToolResult<any>> {
      // Throwing (unknown name, bad arguments) is the contract: the vendored
      // loop converts it into an isError tool result the model can act on.
      const name = isRecord(params) ? params.name : undefined
      if (typeof name !== 'string' || !name.trim()) {
        throw new Error('The "name" argument must be a skill name from the skills index')
      }
      const skill = byName.get(name.trim())
      if (!skill) {
        throw new Error(`Skill "${name.trim()}" not found. Valid skills: ${[...byName.keys()].join(', ')}`)
      }
      const header = skill.description ? `# ${skill.name}\n\n${skill.description}\n\n` : `# ${skill.name}\n\n`
      return {
        content: [{ type: 'text', text: `${header}${skill.body}` }],
        details: {},
      }
    },
  }
  return [tool]
}

/** Lenient SKILL.md frontmatter: a leading `---` block of `key: value` lines. */
function parseFrontmatter(markdown: string): { fields: Record<string, string>, body: string } {
  const match = /^---\r?\n([\s\S]*?)\r?\n---\r?\n?/.exec(markdown)
  if (!match) return { fields: {}, body: markdown }
  const fields: Record<string, string> = {}
  for (const line of match[1].split(/\r?\n/)) {
    const pair = /^([A-Za-z0-9_-]+)\s*:\s*(.*)$/.exec(line)
    if (pair) fields[pair[1].toLowerCase()] = pair[2].trim().replace(/^['"]|['"]$/g, '')
  }
  return { fields, body: markdown.slice(match[0].length) }
}

function firstString(...values: unknown[]): string {
  for (const value of values) {
    if (typeof value === 'string' && value.trim()) return value
  }
  return ''
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
