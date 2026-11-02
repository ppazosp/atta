# Atta v0 plan

Atta is named after leafcutter ants, whose colonies split the work between castes.

Atta v0 lets you send work to agents from Slack and follow it there. You write to Atta in Slack. A lean router decides what to do. Throwaway workers do the work in T3 Code, and you see their progress in the Slack thread until they report back.

This plan says what v0 includes, how it is built, in what order, and how we will know it works. Nothing here is built yet. Building starts when you say so.

- Architecture diagram: [docs/architecture.png](architecture.png) (source in `architecture.dot`)
- Worker lifecycle statechart: [docs/worker-lifecycle.png](worker-lifecycle.png) (source in `worker-lifecycle.dot`)
- Evidence: [T3 as the runtime](proofs/t3-runtime.md), [Slack progress](proofs/slack-progress.md), [router on Pi Durable](proofs/router-pi-durable.md)

## What v0 does

When v0 is done, you can:

1. DM Atta or mention it in a channel, and get a reaction within a second.
2. Ask a question, and get an answer in the thread from the router.
3. Ask for code work on an allowed repository. The router starts a worker in its own worktree.
4. Watch the worker in the thread as a Slack plan message. Each command, file edit or step is a task.
5. Get the result in the same thread when the worker finishes.
6. Reply in the thread while the worker runs. Your reply goes straight to that worker without the router.
7. Say "stop" in the thread, and the worker stops.
8. Have scheduled prompts from the config file run as if you had sent them.
9. Restart your Mac or Atta, and find running work still tracked and reported.

## What v0 leaves out

These are deliberately out of scope. Each one plugs into an interface v0 already has:

- the memory system (v0 uses a placeholder that remembers nothing)
- GitHub and email channels
- messages from anyone except you
- automatic resubmission of interrupted runs, and detection of stuck runs (the watchdog)
- Claude as router or worker
- running anywhere except your Mac

## Decisions already made

| Area                   | Decision                                                                                                       | Evidence                                                                            |
| ---------------------- | -------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| Interface              | Slack, through a Slack app in Socket Mode, so no public webhook                                                | Slack proof                                                                         |
| Progress               | One `plan` message per worker run, edited with `chat.update` at most once a second, then the answer at the end | Slack proof: streams close after 10.1 minutes and turn unfinished tasks into errors |
| Router                 | Atta runs it, on Pi Durable, with `gpt-6.1-terra` at medium, no shell, and only Atta's tools                   | Router proof                                                                        |
| Workers                | T3 Code, unchanged, pinned nightly. `gpt-6.1-sol` at xhigh, one worktree each. They use your own `~/.codex`    | First proof                                                                         |
| Talking to T3          | T3's MCP endpoint, signed in with full access                                                                  | First proof                                                                         |
| Worker callbacks       | Atta's MCP server on localhost, added through T3's worker launch settings                                      | Router proof                                                                        |
| Models                 | One ChatGPT subscription for the router and the workers                                                        | Router proof                                                                        |
| Where it runs          | Your Mac, as background services                                                                               | Your decision                                                                       |
| Language and libraries | TypeScript, Effect 4, SQLite                                                                                   | Your decision                                                                       |

## How it is built

The [architecture diagram](architecture.png) shows the parts. In short:

- Channel adapters turn Slack messages and scheduled prompts into events, and send replies, reactions and plan updates back.
- Core holds the logic: filter, dispatch, router, Atta's tools, worker manager and worker tracker. Core never imports an adapter.
- Five interfaces connect core to the outside: Channel, Router engine, Runtime, Memory and Store.
- Adapters implement them: Slack and Clock, Pi Durable, T3, a placeholder memory, and SQLite.
- One app process wires it all together and runs Atta's MCP server.

### Code layout

One package, not a monorepo. Folders keep the layers apart, and a lint rule stops core from importing adapters.

```text
atta/
  src/
    core/            domain types, filter, dispatch, router, tools, worker manager, worker tracker
    ports/           Channel, RouterEngine, Runtime, Memory, Store (Effect services)
    adapters/
      slack/         Bolt in Socket Mode, plan messages, reactions
      clock/         scheduled prompts from config
      pi-durable/    router engine
      t3/            runtime (MCP client)
      memory-none/
      sqlite/        store and migrations
    mcp/             Atta MCP server
    app/             config, wiring (Layers), main
  test/
  launchd/           service definitions
  atta.config.ts
```

### Interfaces

Each interface is an Effect service. Signatures are a first draft and will tighten when we write them.

- Channel:
  - `events: Stream<InboundEvent>`
  - `reply(thread, text)`
  - `react(message, emoji)`
  - `upsertPlan(thread, planId, title, tasks)`
  - `finishPlan(thread, planId, answer)`
- RouterEngine:
  - `submit({ requestId, content, whenBusy })`
  - `settled(requestId)`
- Runtime:
  - `launch({ repo, title, brief })` returns a worker reference
  - `send(worker, text, "steer" | "queue")`
  - `stop(worker)`
  - `steps(worker, afterPosition)` returns steps and the next position
  - `status(worker)`
- Memory:
  - `recall(query)`
  - `remember(fact)`, which does nothing in v0
- Store: one repository per table (see below), with every write in a transaction.

### Stored data

Atta's SQLite file is the source of truth. Pi Durable keeps only the router's conversation, in its own SQLite file.

| Table                | Holds                                                                                                     | Notes                                                                          |
| -------------------- | --------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `events`             | every inbound event: Slack message, scheduled prompt, worker step summary, worker report, worker finished | append-only. Unique on source and external ID, so a Slack retry is stored once |
| `threads`            | one row per conversation thread, keyed like `slack:T01/D01/1700000000.000100`                             | channel, title, last activity                                                  |
| `workers`            | one row per worker: T3 thread ID, repo, brief, state, last read position, plan message reference          | state follows the statechart                                                   |
| `worker_transitions` | every state change, with reason and time                                                                  | append-only                                                                    |
| `subscriptions`      | which worker listens to which thread                                                                      | many-to-many, as in the article                                                |
| `deliveries`         | where each event went (router or a worker) and whether it arrived                                         | gives at-least-once delivery after a crash                                     |

### Flow of one message

1. The Slack adapter receives your message and stores it as an event. It ignores duplicates by Slack event ID.
2. The filter keeps it only if it comes from your Slack user ID. v0 drops everything else and logs it.
3. Core adds a 👀 reaction straight away, without waiting for a model.
4. Dispatch checks the thread:
   - if a running worker is subscribed, the message goes straight to it as a steer
   - if an idle worker is subscribed, it goes to that worker as a new run
   - otherwise it goes to the router
5. The router gets the event as one input. It answers with tools:
   - `reply` posts in the thread and ends the router's run, which saves the closing model turn the proof showed
   - `start_worker` launches a worker and subscribes it to the thread
6. The worker manager launches the worker through the T3 adapter, and posts the plan message in the thread.
7. The worker tracker reads the worker's steps from T3 by position. It updates the plan message at most once a second.
8. When the run ends, the tracker stores a "worker finished" event with the worker's final message. Dispatch sends it to the router.
9. The router replies in the thread with the result. Core swaps 👀 for ✅.

### The router

The router is a long-lived Pi Durable conversation, with automatic compaction. Events arrive as inputs. New events queue while it works, and a later event about the same thread can steer it.

Its tools are written once in core as Effect programs. The Pi Durable adapter wraps each one in a Pi tool that runs the program:

| Tool                                 | Does                                    | Safe to repeat after a crash                                                                   |
| ------------------------------------ | --------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `reply(thread, text)`                | posts in the thread and ends the run    | yes: the store records each reply before posting it, and a replay skips a reply already posted |
| `start_worker(repo, title, brief)`   | launches and subscribes a worker        | no, a second launch would duplicate it                                                         |
| `message_worker(worker, text, mode)` | steers or queues a message for a worker | no                                                                                             |
| `stop_worker(worker)`                | stops a worker                          | yes                                                                                            |
| `subscribe(thread, worker)`          | links another thread to a worker        | yes                                                                                            |
| `recall(query)`                      | asks memory and state for context       | yes                                                                                            |

The prompt states the rules the proof showed we need:

- briefs must be self-contained and must never name a branch to commit to
- every event that needs your attention gets a `reply`

### Workers

Workers run in T3 with your own Codex setup, including your MCP servers, plugins and skills. Atta adds only two things through T3's launch settings for the worker instance:

- the reasoning effort (xhigh)
- Atta's MCP server (`mcp_servers.atta.url`)

Each brief ends with a fixed footer:

- your worker ID
- use `atta.ask` when you need context
- call `atta.report` when you finish or get blocked
- work on the branch T3 created for you, and do not switch branches

When a worker finishes, the tracker checks which branch it committed to. If it is not the worker's own branch, the tracker flags it in the thread.

### Atta's MCP server

It listens on `127.0.0.1` only. Workers can call two tools:

- `ask(worker_id, question)` is answered from state and, later, memory. It is read-only
- `report(worker_id, text)` becomes an event for the router

v0 trusts the worker ID that the brief gives each worker. Real per-caller authentication comes when other agents use the server.

### Configuration

`atta.config.ts` is typed and checked at startup with Effect Schema. It holds:

- Slack app tokens, read from the macOS Keychain or a `.env` file that git ignores
- your Slack user ID, as the only trusted sender
- allowed repositories: name, local path and T3 project ID
- models and reasoning levels for the router and workers
- scheduled prompts: cron expression, thread, text
- how long an idle worker stays subscribed (default 7 days)

### Running on your Mac

Two launchd services start at login and restart if they exit:

- `com.atta.t3` runs `t3 serve` on `127.0.0.1:3999`, with its data in `~/.atta/t3`
- `com.atta.app` runs Atta, with logs in `~/.atta/logs`

When the Mac sleeps, Slack disconnects and workers pause. Both recover when it wakes: Slack reconnects, and the tracker reads from its last stored position.

### Stack

All versions are pinned exactly.

| Package                                                                        | Version                       | Why                                                                    |
| ------------------------------------------------------------------------------ | ----------------------------- | ---------------------------------------------------------------------- |
| Node                                                                           | 24 LTS (24.13.1 now)          | what T3 and the proofs ran on                                          |
| `effect`, `@effect/platform-node`, `@effect/sql-sqlite-node`, `@effect/vitest` | 4.0.7                         | the 4.0 line T3 uses, which keeps the Codex fallback client compatible |
| `@earendil-works/pi-durable`, `pi-ai`, `chord`, `pi-schema`                    | 1.5.0                         | tested in the router proof. The API changes between releases           |
| `@slack/bolt`                                                                  | 5.1.0                         | tested in the Slack proof                                              |
| `@slack/web-api`                                                               | 8.3.0                         | tested in the Slack proof                                              |
| `@modelcontextprotocol/sdk`                                                    | 1.32.2                        | T3 client and Atta's MCP server, both tested                           |
| `vitest`                                                                       | 5.1.0                         | needed by `@effect/vitest` 4.0.7                                       |
| T3 Code                                                                        | `0.1.7-nightly.20261102.4066` | tested in the first proof. Upgrade on purpose, never automatically     |
| pnpm                                                                           | 12.x (12.5.1 installed)       | package manager                                                        |

## Order of work

The work runs in three waves. Wave 1 fixes the contracts everyone codes against. Wave 2 builds the parts in parallel, each against those contracts and fakes. Wave 3 wires the parts together and runs the Slack checkpoints.

```mermaid
flowchart LR
  WP0[WP0 contracts] --> WP1[WP1 SQLite store]
  WP0 --> WP2[WP2 Slack channel]
  WP0 --> WP3[WP3 T3 runtime]
  WP0 --> WP4[WP4 Pi Durable router engine]
  WP0 --> WP5[WP5 core logic]
  WP0 --> WP6[WP6 config, Clock, memory]
  WP0 --> WP7[WP7 Atta MCP server]
  WP1 & WP2 & WP3 & WP4 & WP5 & WP6 & WP7 --> WP8[WP8 wiring and checkpoints]
```

### Who builds what

Claude (this session) builds the parts that set the design: the contracts, the core logic and the wiring. Codex (`gpt-6.1-sol` at xhigh, through the CLI) builds the adapters in parallel, each in its own worktree. See the [codex-worker skill](../.agents/skills/codex-worker/SKILL.md).

Every package gets a review from the other side before it lands:

- Claude reviews each Codex package
- Codex reviews WP0, WP5 and WP8

At most three Codex workers run at once. That keeps the shared usage limit and the reviews manageable.

### Work packages

| ID  | Package                                                                                                                                                                                                            | Builder | Owns                                                                  | Done when                                                                                                                     |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------- | --------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| WP0 | Contracts: domain types and schemas (event, thread key, worker, brief, step), the five ports as Effect services, the worker statechart as a pure transition function, the config schema, and a fake for every port | Claude  | `src/ports`, `src/core/domain`, `test/fakes`                          | types compile. The statechart test covers every arrow in the [statechart](worker-lifecycle.png). Codex has reviewed it        |
| WP1 | SQLite store: migrations for the 6 tables, one repository per table, transactions, duplicate-event protection                                                                                                      | Codex   | `src/adapters/sqlite`                                                 | tests on a temporary database cover duplicate events, transitions and delivery records                                        |
| WP2 | Slack channel: Bolt in Socket Mode to `InboundEvent`, `reply`, `react`, plan messages with `chat.update` at most once a second, grouping above 50 tasks                                                            | Codex   | `src/adapters/slack`                                                  | tests with a fake Slack client cover deduplication, throttling, grouping and thread keys                                      |
| WP3 | T3 runtime: MCP client with OAuth tokens, `launch`, `send`, `stop`, `steps` by position, `status`                                                                                                                  | Codex   | `src/adapters/t3`                                                     | tests against recorded T3 responses (from the proofs) cover each call, the last-page position and "interrupted" runs          |
| WP4 | Router engine: Pi Durable harness, Atta's tools wrapped as Pi tools, `submit`, `settled`, `reply` ending the run, an `atta login` command for ChatGPT sign-in                                                      | Codex   | `src/adapters/pi-durable`, `src/app/login.ts`                         | tests with Pi's in-memory storage and Pi's `faux` model provider cover tool calls, steering, duplicate request IDs and resume |
| WP5 | Core logic: filter, dispatch, worker manager, worker tracker (steps to plan tasks, finish detection, branch check), the router's tools, the router prompt                                                          | Claude  | `src/core` (except `domain`)                                          | tests with the WP0 fakes cover flow steps 1 to 9 and scenarios 1, 2, 5, 6 and 11                                              |
| WP6 | Config loading, the Clock channel (cron prompts to events) and the placeholder memory                                                                                                                              | Codex   | `src/app/config.ts`, `src/adapters/clock`, `src/adapters/memory-none` | tests with Effect's test clock show each prompt fires once per slot, also after a restart                                     |
| WP7 | Atta's MCP server: `ask` and `report` over HTTP on `127.0.0.1`, scoped to workers                                                                                                                                  | Codex   | `src/mcp`                                                             | tests call both tools with the MCP client and check the events they create                                                    |
| WP8 | Wiring and checkpoints: layers in `src/app`, `main.ts`, launchd services, logs, moving T3's data to `~/.atta/t3`, then the end-to-end scenarios below                                                              | Claude  | `src/app` (except `config.ts` and `login.ts`), `launchd`              | all 11 scenarios pass on your Mac                                                                                             |

### Rules that keep parallel work safe

- WP0 lands before any wave 2 package starts. Changing a port after that needs a pull request of its own, and every open package rebases onto it.
- A package changes only the files it owns. Shared files (`package.json`, `pnpm-lock.yaml`, `knip.json`, `docs/v0-plan.md`) change only in WP0 and WP8. A package that needs a new dependency asks first.
- Every dependency the plan needs is already installed and pinned, so wave 2 needs no lockfile changes.
- Each package lands as one pull request with CI green.

### Slack checkpoints

The milestones become checkpoints in WP8. Each one is something you can try in Slack.

| #   | Checkpoint               | Needs                                                                                          | You can try                                                           |
| --- | ------------------------ | ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| 1   | Events in, reactions out | WP1, WP2, WP5 (filter, dispatch), WP6                                                          | DM Atta and get 👀. The event is stored once, even when Slack retries |
| 2   | Router answers           | WP4, WP5 (router tools)                                                                        | ask a question and get an answer in the thread                        |
| 3   | Workers                  | WP3, WP5 (manager, tracker). T3 moves to `~/.atta/t3`, which needs one new T3 sign-in from you | ask for a fix, watch the plan fill in, get the result                 |
| 4   | Threads                  | WP5 (pass-through, steer, stop), WP7                                                           | reply mid-run and see the worker act on it. Say "stop"                |
| 5   | Running for real         | WP6 (Clock), launchd, recovery                                                                 | restart Atta mid-run and see the plan carry on                        |

## How we will know it works

Each milestone has unit tests that run core against fake adapters. Effect's test clock covers timers, retries and schedules without waiting.

The end-to-end checks run against real Slack, T3 and models. v0 is done when all of these pass on your Mac:

| #   | Scenario                                      | Passes when                                                                                                         |
| --- | --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| 1   | Slack sends the same event twice              | one event row, one router input                                                                                     |
| 2   | Someone else messages Atta                    | nothing happens except a log line                                                                                   |
| 3   | You ask a question                            | 👀 within 1 second, a reply in the thread                                                                           |
| 4   | You ask for a fix on `sandbox-repo`           | the worker runs in its own worktree, on its own branch. The plan shows its steps, the result arrives, 👀 becomes ✅ |
| 5   | You reply in the thread while the worker runs | the worker gets it as a steer. The router does not run                                                              |
| 6   | You say "stop"                                | the worker stops, and the plan shows it stopped                                                                     |
| 7   | `kill -9` Atta while a worker runs            | after restart, the plan carries on from the last step and the result still arrives                                  |
| 8   | Restart T3 while a worker runs                | the plan marks the task as failed, and the thread says the run was interrupted                                      |
| 9   | A worker runs longer than 10 minutes          | the plan message keeps updating, because it does not use a stream                                                   |
| 10  | A scheduled prompt is due                     | it runs once, in its configured thread                                                                              |
| 11  | A worker commits to `main`                    | the thread flags it                                                                                                 |

## Risks

| Risk                                                                                                                                      | What we do                                                                                          |
| ----------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Pi Durable is experimental and its API changes                                                                                            | pin 1.5.0 exactly, keep it behind the RouterEngine interface, keep Codex app-server as the fallback |
| T3 nightly changes its MCP tools                                                                                                          | pin the version, and keep T3 calls in one adapter with a contract test                              |
| Router and workers share one usage limit                                                                                                  | the router uses short turns on terra. Log usage per event so we can see it                          |
| Your `~/.codex` plugins slow workers down, by about 2.5 seconds per turn in the first proof                                               | accepted. Worker runs are long, so it matters little. Tune your plugins separately if you want      |
| Slack allows 50 blocks per message and 50 tasks per stream (from Slack's docs, not measured for a `plan` block edited with `chat.update`) | group steps when a run has more than 50, and measure the real limit in milestone 3                  |
| A worker given full tools could act on text from strangers                                                                                | v0 accepts only your messages. Later, untrusted events always go through the router                 |

## Decisions still open, with defaults

v0 starts with these defaults unless you change them:

- repositories: `sandbox-repo` first, then the real repositories you list
- Slack app: a new app called Atta, from the proof's manifest, in the same workspace as the proof
- code repository: private on GitHub until v0 works
- idle workers stay subscribed for 7 days

Before any code, step 1 is to turn the interface signatures and the statechart into types, and walk them through scenarios 4 to 8 on paper.
