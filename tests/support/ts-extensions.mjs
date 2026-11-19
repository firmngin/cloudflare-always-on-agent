/**
 * Resolve hook for running the Worker's TypeScript sources under `node --test`.
 *
 * The Worker is bundled (moduleResolution "bundler"), so `src/` imports its own
 * modules without an extension. Node's ESM resolver requires one, so a relative
 * specifier that fails to resolve is retried with `.ts`. Runtime-only modules
 * (`cloudflare:workers`) resolve to a local stub: the tests exercise the Worker
 * entrypoint, never the Durable Object runtime.
 */
const CLOUDFLARE_RUNTIME_STUB = new URL('./cloudflare-workers-stub.mjs', import.meta.url).href

export async function resolve(specifier, context, next) {
  if (specifier.startsWith('cloudflare:')) {
    return { url: CLOUDFLARE_RUNTIME_STUB, shortCircuit: true }
  }
  try {
    return await next(specifier, context)
  } catch (error) {
    if (error?.code !== 'ERR_MODULE_NOT_FOUND') throw error
    if (!specifier.startsWith('.') && !specifier.startsWith('/')) throw error
    if (/\.[cm]?[jt]s$/.test(specifier)) throw error
    return next(`${specifier}.ts`, context)
  }
}
