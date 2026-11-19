/**
 * Minimal stand-in for the `cloudflare:workers` runtime module, so the Worker
 * sources (and the `agents` package they import) can be loaded under
 * `node --test`. Only names imported at module scope are needed: the tests
 * never construct a Durable Object or an entrypoint.
 */
export class DurableObject {
  constructor(ctx, env) {
    this.ctx = ctx
    this.env = env
  }
}

export class RpcTarget {}

export class WorkerEntrypoint {
  constructor(ctx, env) {
    this.ctx = ctx
    this.env = env
  }
}

export class WorkflowEntrypoint {}

export const exports = {}
