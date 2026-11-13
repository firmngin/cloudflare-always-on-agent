# Firmngin data-agent Worker

User-owned Cloudflare Worker that executes a Firmngin **data agent**: it
long-polls the agent inbox, claims queued turns, runs them against any
OpenAI-compatible model endpoint **with your own provider key** (BYOK), streams
tool calls and deltas back to Firmngin, and reports the final result.

Firmngin stores the agent config and conversation history and relays the
stream to the dashboard. It never sees your model provider key and never
executes the agent.

## Deploy with one click

From the Firmngin dashboard (**AI Agents → Data Agents → agent settings →
Access**) click **Deploy to Cloudflare**. The wizard asks for:

| Field | Where to get it |
|---|---|
| `AGENT_ID` (var) | Firmngin dashboard, shown next to the deploy button |
| `AGENT_TOKEN` (secret) | the `fng_aoa_...` token — create one in the same tab; it is shown once |
| `MODEL_API_KEY` (secret) | your model provider key (OpenRouter, OpenAI, ...) |

The Worker deploys with an hourly cron trigger. Change the schedule in the
Cloudflare dashboard (**Workers → your worker → Triggers → Cron**).

## Manual deploy

```bash
npm install
npx wrangler secret put AGENT_TOKEN
npx wrangler secret put MODEL_API_KEY
npx wrangler deploy
```

## What it does on each cron tick

1. Sends a heartbeat (drives the "Worker connected" status in the dashboard).
2. Queues one scheduled turn (hour-bucket idempotency key, so a retry within
   the same hour does not duplicate the run).
3. Drains the inbox: claims each queued turn (interactive chat replies and
   scheduled turns run through the same loop), executes it with tool access,
   streams deltas/tool events, and reports the final answer.

`POST`ing to the Worker URL drains the inbox immediately — handy for testing
right after deploying. `GET`ing it sends a heartbeat and reports the version.

## Tools (MCP)

If the agent has MCP enabled in Firmngin, the Worker exchanges its agent token
for a short-lived MCP token and exposes the allowed tools to the model.
Scopes are read-only by default; control/manage scopes must be opted into on
the agent in the Firmngin dashboard.

## Limits

- A turn is abandoned (reported `failed`) after 4 minutes — Firmngin's own
  deadline is 5.
- At most 10 turns are claimed per invocation; the rest wait for the next tick.
- Model calls use streaming with `tools` in OpenAI format; the model id from
  the agent config is passed to the provider unchanged.
