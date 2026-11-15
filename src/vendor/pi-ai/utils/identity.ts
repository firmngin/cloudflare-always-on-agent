// Vendored from pi (packages/ai/src/utils/identity.ts) with Node-only pieces
// removed for the Cloudflare Workers runtime: the models.ts catalog types and
// the process-based User-Agent formatting. See ../pi/NOTICE.
import { PI_AI_VERSION } from "./version.ts";

/**
 * The `User-Agent` sent to providers:
 * - `"detailed"`: `pi/<version> (<platform>; <runtime>/<runtime version>; <arch>)`, or `pi/<version> (browser)`
 *   outside Node.js and Bun.
 * - `"short"`: `pi`.
 * - `{ custom }`: the given value.
 */
export type UserAgentMode = "short" | "detailed" | { custom: string };

/** Who requests are attributed to. `getAttributionHeaders()` maps the fields to each provider's headers. */
export interface ClientAttribution {
	/** App name: OpenRouter app title, NVIDIA invoke origin (first letter upper-cased), OpenCode client. */
	name: string;
	/** App URL: OpenRouter `HTTP-Referer`, the app's identity in OpenRouter's rankings, grouped by origin. */
	url: string;
	/** App categories, e.g. `["cli-agent"]`: OpenRouter `X-OpenRouter-Categories`, comma-separated. */
	categories?: string[];
}

/** pi's attribution, used by pi-agent-core, pi-durable, and the coding agent. */
export const PI_ATTRIBUTION: ClientAttribution = { name: "pi", url: "https://pi.dev", categories: ["cli-agent"] };

/**
 * How pi-ai identifies the calling app to providers. Every field is optional; absent fields use pi-ai's defaults:
 * a detailed `User-Agent` with pi-ai's version and no attribution headers. Plain JSON, so it can be persisted.
 */
export interface ClientIdentity {
	/** Default: `"detailed"`. */
	userAgent?: UserAgentMode;
	/** Provider attribution headers. Absent or `null`: none; `null` also turns off attribution set by a merged identity. */
	attribution?: ClientAttribution | null;
	/** Version in the detailed `User-Agent`. Default: the pi-ai version. */
	version?: string;
}

/** The provider and endpoint a request goes to. */
export interface ClientIdentityTarget {
	readonly provider: string;
	readonly baseUrl: string;
}

const NVIDIA_NIM_HOST = "integrate.api.nvidia.com";
const OPENCODE_HOST = "opencode.ai";

/** Merge identities field by field; later sources win and undefined fields are skipped. */
export function mergeClientIdentity(...sources: (ClientIdentity | undefined)[]): ClientIdentity | undefined {
	let merged: ClientIdentity | undefined;
	for (const source of sources) {
		if (!source) continue;
		merged = { ...merged };
		if (source.userAgent !== undefined) merged.userAgent = source.userAgent;
		if (source.attribution !== undefined) merged.attribution = source.attribution;
		if (source.version !== undefined) merged.version = source.version;
	}
	return merged;
}

/** The version in the detailed `User-Agent`. */
export function getClientVersion(identity?: ClientIdentity): string {
	return identity?.version ?? PI_AI_VERSION;
}

function matchesHost(baseUrl: string, expectedHost: string): boolean {
	try {
		return new URL(baseUrl).hostname === expectedHost;
	} catch {
		return false;
	}
}

function isOpenRouter(target: ClientIdentityTarget): boolean {
	return target.provider === "openrouter" || target.baseUrl.includes("openrouter.ai");
}

function isNvidiaNim(target: ClientIdentityTarget): boolean {
	return target.provider === "nvidia" || matchesHost(target.baseUrl, NVIDIA_NIM_HOST);
}

function isOpenCode(target: ClientIdentityTarget): boolean {
	return (
		target.provider === "opencode" || target.provider === "opencode-go" || matchesHost(target.baseUrl, OPENCODE_HOST)
	);
}

/** Provider attribution headers of an identity for a request target. Empty when attribution is off. */
export function getAttributionHeaders(target: ClientIdentityTarget, identity?: ClientIdentity): Record<string, string> {
	const attribution = identity?.attribution;
	if (!attribution) return {};
	const { name, url, categories } = attribution;
	if (isOpenRouter(target)) {
		return {
			"HTTP-Referer": url,
			"X-OpenRouter-Title": name,
			...(categories === undefined || categories.length === 0
				? {}
				: { "X-OpenRouter-Categories": categories.join(",") }),
		};
	}
	// NVIDIA expects a capitalized origin, such as `Pi`.
	if (isNvidiaNim(target)) return { "X-BILLING-INVOKE-ORIGIN": name.charAt(0).toUpperCase() + name.slice(1) };
	if (isOpenCode(target)) return { "x-opencode-client": name };
	return {};
}

/**
 * OpenCode's per-conversation routing header for requests to OpenCode, including custom providers pointing at it.
 * Undefined without a session or for other targets.
 */
export function getOpenCodeSessionHeaders(
	target: ClientIdentityTarget,
	sessionId: string | undefined,
): Record<string, string> | undefined {
	return sessionId && isOpenCode(target) ? { "x-opencode-session": sessionId } : undefined;
}
