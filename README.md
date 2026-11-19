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
| `WAKE_SECRET` (secret) | an independent random secret of at least 32 bytes |
| `WORKER_BASE_URL` (variable) | this Worker's public HTTPS origin, e.g. `https://worker.example.com` — no path |
| `WAKE_CALLBACK_URL` (variable, legacy) | the full HTTPS route ending exactly in `/v1/wake`; used only when `WORKER_BASE_URL` is absent |

There is no agent id and no agent form: the Worker is workspace-level.

The Worker deploys with a once-a-minute recovery cron. Realtime provisioning is
not complete until the custom route, a wake URL (`WORKER_BASE_URL` for new
deployments, `WAKE_CALLBACK_URL` for existing ones), and `WAKE_SECRET` exist and
the authenticated handshake succeeds. The public wake route remains
fail-closed before that point, while existing outbound cron recovery continues.
Firmngin then calls only signed `POST /v1/wake` for realtime work. For recovery,
each cron invocation keeps long-polling the inbox
for up to ~59 seconds instead of quitting after one empty poll, so a waiter is
registered nearly continuously. Actual pickup still depends on Cloudflare cron
scheduling, network latency, backend processing, and the model provider.
An idle workspace with enabled agents also holds a bounded request, so the
first chat can wake it. No-agent or over-cap immediate empty responses end the
drain until the next cron tick; there is no rapid empty-request retry loop.
Change the schedule in the Cloudflare dashboard (**Workers → your worker →
Triggers → Cron**).

`workers_dev` and `preview_urls` remain disabled. The former unauthenticated
`POST /` drain is closed; only `/v1/wake` accepts POST and requires a signed,
timestamped, replay-protected request. Provisioning must create a real HTTPS
custom route—the Worker name is never converted into a guessed URL.

## Manual deploy

```bash
npm install
npx wrangler secret put WORKER_TOKEN
npx wrangler secret put MODEL_API_KEY
npx wrangler secret put WAKE_SECRET
# Create the HTTPS custom route, then set the Worker's public origin as
# WORKER_BASE_URL in the Worker variables to enable realtime wake (the runtime
# derives /v1/wake itself). Existing deployments may keep the legacy
# WAKE_CALLBACK_URL=<origin>/v1/wake instead.
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
   streams deltas/tool events, and reports the final answer. Capacity is per
   lane, from the backend-assigned one: 2 interactive turns and 1 background
   turn (scheduled/manual/analysis) per isolate, so an analysis cannot hold a
   chat slot. A poll plans both lanes before starting anything and dispatches
   them concurrently, so a saturated background lane cannot delay chat pickup in
   the same poll.
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

The repository deliberately contains no guessed public callback URL. Until
provisioning supplies a custom route plus `WORKER_BASE_URL` (or the legacy
`WAKE_CALLBACK_URL`) and `WAKE_SECRET`, `POST /v1/wake` returns `503`; the
existing outbound cron and bounded long-poll remain available as staged-rollout
recovery.

## Pairing and runtime config

On each cron recovery invocation, the Worker sends an authenticated outbound
`POST /handshake`. Once the wake settings are valid it includes the resolved
full `wake_url` and reverse secret; Firmngin encrypts the secret at rest and
never returns it. Missing or partially staged wake settings do not stop the
existing cron fallback, but the inbound wake route remains disabled.

Wake URL resolution has exactly one rule (`src/runtime/wake-route.ts`), shared
by the handshake, the config gates, and the `GET /` diagnostic:

- `WORKER_BASE_URL` set and valid — a bare HTTPS origin, optional trailing
  slash, no path/query/hash/credentials. The reported URL is
  `<origin>/v1/wake`. It is authoritative, so a stale legacy value is ignored.
- `WORKER_BASE_URL` set but invalid — fail closed: waking stays disabled and
  the runtime logs the cause. It never silently reuses `WAKE_CALLBACK_URL`.
- `WORKER_BASE_URL` absent or blank — fall back to a valid legacy
  `WAKE_CALLBACK_URL` (HTTPS, path exactly `/v1/wake`).

The handshake payload, signed path, `WAKE_SECRET`, the Durable Object binding
and the backend contract are unchanged; only the local source of the URL
differs. `wake_url_source` in the `GET /` response reports which one won.
Its agent roster comes from `GET /deployment/config`, which is scoped to the
Worker token and carries the workspace id needed for the Durable Object name.
Both outbound routes use the worker token. The reverse direction is limited to
the separately signed, replay-protected `POST /v1/wake`; the model provider key
never leaves Cloudflare.

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
- At most 10 turns are claimed per invocation and at most 2 chat turns (plus 1
  background turn) run at once; the rest wait for the next tick.
- **Subrequests are budgeted per invocation.** A Worker invocation may make 50
  subrequests on the Workers Free plan and 10,000 on Paid, and each hop of a
  redirect chain counts as one more, so the platform total can exceed the number
  of `fetch()` calls in the code. The limit is not visible to the code, so the
  runtime assumes the Free floor and gives **every invocation its own budget**:
  one cron tick (inside its `ctx.waitUntil`) and one streaming `/v1/wake` request
  each hold an isolated counter, so overlapping invocations on one isolate can
  neither refill nor poison each other. Inside an invocation it spends in three
  tiers: best-effort event batches and retries first (coalesced by 32 events /
  4 KB / 300 ms, first delta still flushed immediately), essential work (claims,
  model steps, MCP calls, inbox long-polls) next, and the run's terminal report
  last. Every run reserves its own terminal headroom (two PATCH attempts plus one
  optional `message.final`/`error` batch) **before** its claim, so up to three
  concurrent runs can never spend each other's report slots. When the budget
  runs out the turn still reports `succeeded` or `failed` and the drain ends
  instead of failing its next poll; the queued turn is picked up by the next cron
  tick. A failed event batch is re-queued, not dropped, so `seq` stays monotonic,
  and an ambiguous report is retried as the idempotent PATCH only — paid
  model/tool work is never re-run just to resend an acknowledged finish.
- A finished turn's terminal report counts as done only when Firmngin answers
  **2xx**. An already-final run replays as 2xx (`FinishRun` re-reads the terminal
  row), but **409 is not an ack**: it means the run is still `queued` or the
  agent moved to the legacy executor, so the report was not applied. On any
  non-2xx (`ambiguous`, `unsent`, `rejected`) the finished payload is kept on the
  agent's Durable Object (`pending_reports_v1`) and the wake event stays open, so
  the backend's retry replays that report instead of re-entering the model turn
  (`ClaimMessage` would resume the same claim). A pending report expires after
  30 minutes; past that the backend stale-run reaper owns the run.
- A deployment that raised `[limits] subrequests` (see `wrangler.toml`) must
  mirror that number in the `SUBREQUEST_BUDGET` var. Leave both unset on the Free
  plan. Setting it above the real platform value is not a guarantee: the runtime
  keeps a small redirect headroom and stops on the first platform rejection, but
  it cannot predict a limit it cannot read.
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
