# Firmngin data-agent Worker

User-owned Cloudflare Worker that executes **every data agent of your
workspace**: it long-polls the merged inbox, claims queued turns, runs them
against any OpenAI-compatible model endpoint **with your own provider key**
(BYOK), streams tool calls and deltas back to Firmngin, and reports the final
result.

Firmngin stores the agent config and conversation history and relays the
stream to the dashboard. It never sees your model provider key and never
executes the agent.

One deployment serves the whole workspace — create agents in the Firmngin
dashboard afterwards and they run here automatically; no redeploy needed.

## Deploy with one click

From the Firmngin dashboard (**AI Agents → Always-On Agent → Deploy Worker**)
click **Deploy to Cloudflare**. The wizard asks for:

| Field | Where to get it |
|---|---|
| `WORKER_TOKEN` (secret) | the workspace Worker token (`fng_aow_...`) — shown once next to the deploy button |
| `MODEL_API_KEY` (secret) | your model provider key (OpenRouter, OpenAI, ...) |

There is no agent id and no agent form: the Worker is workspace-level.

The Worker deploys with a once-a-minute cron trigger. Cron is only the wake-up:
`workers_dev`/preview URLs are disabled so Firmngin can never call into your
Worker. To keep pickup fast, one cron invocation keeps long-polling the inbox
for ~50 seconds instead of quitting after one empty poll, so a waiter is
registered almost continuously and a sent message is claimed within seconds.
An idle workspace with enabled agents also holds a bounded request, so the
first chat can wake it. No-agent or over-cap immediate empty responses end the
drain until the next cron tick; there is no rapid empty-request retry loop.
Change the schedule in the Cloudflare dashboard (**Workers → your worker →
Triggers → Cron**).

`workers_dev` and `preview_urls` are disabled: the Worker exposes an
unauthenticated `POST /` that drains the inbox, so it must not be reachable on a
public hostname.

## Manual deploy

```bash
npm install
npx wrangler secret put WORKER_TOKEN
npx wrangler secret put MODEL_API_KEY
npx wrangler deploy
```

`URL_BACKEND` in `wrangler.toml` points at the production API. It is committed
as production only.

## Local development against a private backend

A development deployment overrides the backend at runtime, so no tunnel URL is
ever committed:

```bash
# vars for this deployment only
npx wrangler deploy --env development
# or, on the plain Worker: add ENVIRONMENT=development and URL_BACKEND_DEV
```

| Variable | Meaning |
|---|---|
| `ENVIRONMENT` | `development` enables the override. Unset or any other value means production. |
| `URL_BACKEND_DEV` | Dev-only API base URL. Ignored unless `ENVIRONMENT=development`. |

For `wrangler dev`, put both values in `.dev.vars` (git-ignored):

```ini
ENVIRONMENT=development
URL_BACKEND_DEV=https://your-dev-backend
```

`GET /` reports the resolved `environment` and `backend`, so you can confirm
which backend a deployment is actually talking to.

## What it does on each cron tick

1. Sends a heartbeat (drives the "Worker connected" status in the dashboard).
2. Lists the workspace's agents and syncs each agent's **Durable Object**
   schedules (`AGENT_DO_SYNC=true`). An agent with at least one configured
   schedule is owned by its object and is no longer queued from cron; an agent
   with no schedule keeps the legacy hour-bucketed scheduled turn. While the
   flag is `false` the legacy path applies to every agent. There is never more
   than one scheduler per agent.
3. Drains the merged inbox: claims each queued turn (interactive chat replies
   and scheduled runs go through the same loop), executes it with tool access,
   streams deltas/tool events, and reports the final answer.
4. Keeps long-polling the inbox until the invocation budget is spent: a workspace
   with enabled agents holds its request until something is enqueued (or up to
   25 seconds), even while idle. Each poll and its aborting timeout fit inside
   the remaining budget. No-agent/over-cap immediate empty responses end the
   drain and use the next cron tick as fallback.

Deploy the backend inbox update before this Worker version. Older Workers can
use the updated backend; this version treats immediate empty responses as a
cron fallback, so an older activity-gated backend can delay an idle first chat
until the next minute. Idle waits remain bounded by the backend timeout and
its shared waiter admission cap.

## Scheduled runs (Durable Object per agent)
Each agent gets one named SQLite Durable Object (`w:{workspace_id}:a:{agent_id}`)
that owns its recurring schedules through the Cloudflare Agents Lifecycle +
Scheduler (`src/durable/agent.ts`). The object never runs the model: a fired
schedule becomes one idempotent enqueue call
(`schedule_id` + `occurrence_at`) and Firmngin dedupes the occurrence, so a
replayed callback cannot create a second run. The binding and migration are
declared in `wrangler.toml`; `npm install` pulls `agents@0.33.0`
(`nodejs_compat` is enabled for it). See
`firmnginbe/docs/architecture/data-agent-durable-objects.md` for the contract.

The default deployment has no public probe URL: `workers_dev` and previews are
disabled. Trace deployment through scheduled invocation logs below. The existing
GET/POST handler is usable only if you already operate a deliberately configured
private route; do not expose it publicly to troubleshoot pairing.

## Pairing and runtime config

On every cron invocation the Worker sends an outbound `POST /handshake`
(`runtime_version` + a correlation nonce) so the workspace deployment moves
`pending → connected` explicitly; repeating it is a no-op except for liveness.
Its agent roster comes from `GET /deployment/config`, which is scoped to the
Worker token and carries the workspace id needed for the Durable Object name.
Both are worker-token routes — Firmngin still never calls into your Worker.

## Tools (MCP)

If an agent has MCP enabled in Firmngin, the Worker exchanges its worker
context for that agent's short-lived MCP token and exposes the allowed tools
to the model. Scopes are read-only by default; control/manage scopes must be
opted into on the agent in the Firmngin dashboard.

## Agent engine (pi core)

Claimed turns run on the [pi](https://github.com/earendil-works/pi) agent
core, vendored under `src/vendor/` (MIT — see `src/vendor/pi/NOTICE`): the
agent loop with tool calling, argument validation/coercion via pi-schema, MCP
tools mapped to pi `AgentTool`s, and skills — a claim may carry `skills`
(name/description/content or raw SKILL.md); their index is appended to the
system prompt and a builtin `load_skill(name)` tool serves the full content on
demand (progressive disclosure). The BYOK provider adapters are unchanged: the
provider key lives only in Worker secrets and is never sent to Firmngin. Set
the `AGENT_ENGINE` var to `builtin` to fall back to the previous hand-rolled
loop (`executeTurnBuiltin`).

## Limits

- A turn is abandoned (reported `failed`) after 4 minutes — Firmngin's own
  deadline is 5.
- At most 10 turns are claimed per invocation and at most 2 run at once; the
  rest wait for the next tick.
- Model calls use streaming with `tools` in OpenAI format; each agent's model
  id is passed to the provider unchanged.

## Trace a deployment that stays on “Waiting for the first heartbeat”

Build success does not prove a cron invocation ran. This version enables
Workers Logs and emits structured registration diagnostics. Open the Worker
in Cloudflare → **Observability** and filter `event` or `invocation_id`.

1. `runtime.invocation.start`: proves the scheduled handler ran; inspect
   `worker_version`, `cron`, `scheduled_at`, `schedule_lag_ms`, `backend_origin`,
   `backend_source`, `environment`, and configuration booleans. An ignored
   development override is explicit in `dev_override_ignored`.
2. `runtime.heartbeat.start` / `runtime.handshake.start`: use their `trace_id`
   to find the same request in Firmngin backend logs (`X-Trace-Id`).
3. `runtime.heartbeat.response`: inspect `status`, `error_code`, `cf_ray`,
   `backend_request_id`, `duration_ms`, and `persistence_ack`. A 2xx response
   without a valid heartbeat timestamp is not persistence confirmation.
4. `runtime.*.failed`: timeout or transport/body failure is logged within 15s;
   token values, headers, raw response bodies, and exception messages are not.
5. A confirmed persistence acknowledgment with a waiting dashboard moves the
   investigation to backend status selection, MQTT publish/subscription, and
   frontend workspace state. Match the exact workspace and backend environment.

No `runtime.invocation.start` for the deployed version means there is no code
evidence that cron ran: inspect the active deployment, Cron Triggers, and
Cloudflare invocation failures. A new/changed trigger can take up to 15 minutes
to propagate; waiting beyond that is not explained by the one-minute schedule.
The diagnostic trace adds no public endpoint; workers.dev remains disabled.

Sources: [Workers Logs](https://developers.cloudflare.com/workers/observability/logs/workers-logs/),
[Cron Triggers](https://developers.cloudflare.com/workers/configuration/cron-triggers/).
