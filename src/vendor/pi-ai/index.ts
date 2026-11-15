// Minimal entry point for the vendored pi-ai sources (see ./NOTICE relative to
// the pi vendor directory). The vendored pi-agent core imports the pi-ai
// PACKAGE by name upstream; here that specifier is rewritten to this barrel,
// which re-exports exactly the symbols the vendored core (and the Worker's
// adapter layer) needs from the vendored files below.

export type {
	Api,
	AssistantMessage,
	AssistantMessageEvent,
	Context,
	ImageContent,
	JsonValue,
	JsonObject,
	Message,
	Model,
	SimpleStreamOptions,
	StopReason,
	SystemMessage,
	TextContent,
	ThinkingContent,
	Tool,
	ToolCall,
	ToolReference,
	ToolResultMessage,
	TranscriptContext,
	Usage,
	UserMessage,
} from "./types.ts";
export type { ClientAttribution, ClientIdentity } from "./utils/identity.ts";
export {
	AssistantMessageEventStream,
	EventStream,
	createAssistantMessageEventStream,
} from "./utils/event-stream.ts";
export type { ToolStateChanges } from "./utils/transcript.ts";
export {
	getCurrentTools,
	getToolStateChanges,
	getInitialSystemMessage,
	normalizeContext,
	toToolDeclaration,
} from "./utils/transcript.ts";
export { contentText } from "./utils/text.ts";
export { PI_ATTRIBUTION, mergeClientIdentity } from "./utils/identity.ts";
export { validateToolArguments } from "./utils/validation.ts";
