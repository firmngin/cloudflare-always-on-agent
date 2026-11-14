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

The Worker deploys with a once-a-minute cron trigger. Inbox pickup is
adaptive: while the workspace is active the long-poll returns the moment a
turn is queued, and while it is idle the backend answers instantly so the
cron tick (within a minute) stays the fallback. Change the schedule in the
Cloudflare dashboard (**Workers → your worker → Triggers → Cron**).

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
2. Lists the workspace's enabled agents and queues one scheduled turn per
   agent. The idempotency key is hour-bucketed, so the per-minute tick reuses
   the same run within an hour instead of creating duplicates.
3. Drains the merged inbox: claims each queued turn (interactive chat replies
   and scheduled runs go through the same loop), executes it with tool access,
   streams deltas/tool events, and reports the final answer.
4. Keeps long-polling the inbox: an active workspace's request is held until
   something is enqueued (or up to 25 seconds), an idle one is answered
   instantly, so a chat reply usually starts within seconds of being sent
   rather than at the next minute boundary; while the workspace is idle the
   once-a-minute cron tick remains the fallback.

`POST`ing to the Worker URL drains the inbox immediately — handy for testing
right after deploying. `GET`ing it sends a heartbeat and reports the version.

## Tools (MCP)

If an agent has MCP enabled in Firmngin, the Worker exchanges its worker
context for that agent's short-lived MCP token and exposes the allowed tools
to the model. Scopes are read-only by default; control/manage scopes must be
opted into on the agent in the Firmngin dashboard.

## Limits

- A turn is abandoned (reported `failed`) after 4 minutes — Firmngin's own
  deadline is 5.
- At most 10 turns are claimed per invocation and at most 2 run at once; the
  rest wait for the next tick.
- Model calls use streaming with `tools` in OpenAI format; each agent's model
  id is passed to the provider unchanged.
