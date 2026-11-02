# atta

atta lets you send work to coding agents from Slack and follow it there. A lean router reads events and decides what to do. Throwaway workers do the work in T3 Code and report back in the Slack thread.

atta is named after leafcutter ants, whose colonies split the work between castes.

The design and the v0 scope are in [docs/v0-plan.md](docs/v0-plan.md). Read it before changing behaviour. The evidence behind each decision is in [docs/proofs](docs/proofs).

## Words we use

- event: one thing that happened (a Slack message, a scheduled prompt, a worker step, a worker report). Stored once, never edited.
- thread: one conversation that events belong to, keyed like `slack:T01/D01/1700000000.000100`.
- router: the lean agent that reads events and decides. It has no shell, only atta's tools.
- worker: a throwaway coding agent in T3 Code. It has no memory and works in its own worktree.
- brief: the self-contained instructions the router gives a worker.
- plan message: the Slack message that shows a worker's steps as tasks.
- channel, router engine, runtime, memory, store: the five interfaces in `src/ports`.

## Where code lives

- `src/core`: domain logic. Never imports `src/adapters`, `src/app`, `src/mcp` or any vendor library. `pnpm lint` enforces this.
- `src/ports`: the five interfaces, as Effect services.
- `src/adapters/<name>`: one folder per implementation (Slack, Clock, Pi Durable, T3, SQLite, memory).
- `src/mcp`: atta's MCP server for workers.
- `src/app`: config, wiring of layers, and `main.ts`.
- `test`: tests that span modules. Unit tests sit next to their code as `*.test.ts`.
- `.repos`: read-only source of Effect and Pi at the pinned versions. Run `pnpm sync:repos` to fetch them. Read them for patterns. Never import from them or edit them.

## How we write code

- Use Effect for everything with effects. Read `.repos/effect/LLMS.md` before writing Effect code, and copy patterns from `.repos/effect` rather than inventing them.
- Each interface is a `Context.Service`. Each adapter is a `Layer`. Only `src/app` builds layers.
- Name reusable effects with `Effect.fn("Service.operation")`. Use `Effect.gen` for inline programs.
- Decode every value that crosses a boundary (Slack payloads, T3 responses, config, MCP input) with Effect Schema.
- Errors are tagged errors in the error channel. Do not throw, except inside third-party callbacks that require it.
- Complexity belongs in adapters. Core stays plain and testable.
- Pi Durable code lives only in `src/adapters/pi-durable`. Its docs are in `.repos/pi/packages/durable/README.md`. Its API changes between releases, so keep the version pinned.
- Every dependency is pinned to an exact version. Effect packages stay on one version together (see `pnpm-workspace.yaml`).

## Things that hurt

1. Killing by pattern. Never use `pkill -f` or kill a PID found by matching a name or path. Your own process and the user's apps match too. Kill only a PID you captured when you started it.
2. Touching the user's live data. Never write to `~/.t3`, `~/.codex` or `~/.atta`. Development and tests use `./data` (gitignored).
3. Leaking secrets. This repository is public. Never commit tokens, `.env` files, Slack IDs from a real workspace, or logs. Never print a token in output.
4. Spending model calls in tests. Unit tests use fake adapters. Only the end-to-end scenarios in the plan call real models, and only when someone runs them on purpose.

## Checks

`pnpm check` runs formatting, lint, types, tests and unused-code detection. It must pass before a pull request, and CI runs the same steps.

Before committing, run `pnpm fmt` to format.

## Git and pull requests

- Branch from `main` with `git switch --no-track -c <branch> origin/main` (or `main` before the first push). Never track `main`.
- One pull request per work package in [docs/v0-plan.md](docs/v0-plan.md#work-packages). Fill in the template: what and why, and the checks you ran.
- Pull request titles follow Conventional Commits and say what changed for the user: `feat: the router answers questions in the thread`, `fix: …`, `docs: …`, `chore: …`. Pull requests are squash-merged, so the title becomes the commit on `main`.
- Do not add `Co-Authored-By` lines.

## Documentation

- When a decision in `docs/v0-plan.md` changes, rewrite the affected text. Do not append a second account.
- A reason the code cannot show goes in a short comment next to the code.
- Write docs in plain English: short sentences, active voice, no bold for emphasis.
- Write the name as atta, in lowercase, even at the start of a sentence.

## Working with Codex

We use the Codex CLI as a second pair of hands, for building work packages in parallel and for reviews. See the [codex-worker skill](.agents/skills/codex-worker/SKILL.md).
