# Physalia proof: the router on Pi Durable

> Written during the proofs, when the project was called Physalia and lived outside this repository. Paths like `~/projects/physalia/poc` refer to that proof folder, which is not published.

Run on 2 November 2026 on Pablo's Mac with `@earendil-works/pi-durable` 1.5.0 and `@earendil-works/pi-ai` 1.5.0
(pinned exactly), Node 24.13.1, and T3 Code nightly `0.1.7-nightly.20261102.4066` for the worker.
Every claim says whether it was **measured** here, **inferred** or a **guess**.

## Summary

Pi Durable works as the engine for Physalia's own router.

- The router runs on Pablo's ChatGPT subscription through Pi's "Sign in with ChatGPT", on `gpt-6.1-terra` at medium,
  with **only Physalia's three tools**: no shell, no files (measured).
- Steering, crash recovery and duplicate protection all work without any code of ours (measured).
- A real T3 worker reached **Physalia's MCP server** through the worker's launch settings, without touching
  `~/.codex` (measured). The worker asked a question and reported its result over that server, and the router turned
  the report into a Slack reply 3 s later (measured).

## Setup

- `poc/router-pi/`: `creds.mjs` (file-backed credential store at `~/.physalia/pi-auth.json`, mode 0600, plus a
  stable device ID at `~/.physalia/pi-device-id`), `login.mjs` (sign-in), `router.mjs` (all the tests).
- Router storage: one SQLite file per test under `data/`.
- Router tools: `start_worker(repo, title, brief)`, `reply(thread, text)`, `recall(query)`. `recall` and `reply`
  are marked safe to repeat after a crash. In every test except the last, `start_worker` is fake.
- Worker: T3's `codex-worker` instance with
  `-c mcp_servers.physalia.url="http://127.0.0.1:8790/mcp"` added to its launch settings in T3's `settings.json`.

## Results

| #   | Question                                               | Answer                       | Evidence                                                                                                                                                                                                                                     |
| --- | ------------------------------------------------------ | ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Does Sign in with ChatGPT work for Pi?                 | Yes                          | OpenAI's agent sign-in (`dynamic_agent_client`, agent name "Pi"); the token was saved and used for every test. Pi requires a stable device ID per installation, which the first attempt lacked (`Sign in with ChatGPT requires a device ID`) |
| 2   | Is `gpt-6.1-terra` available that way?                 | Yes                          | All tests ran on `openai/gpt-6.1-terra`, thinking `medium`                                                                                                                                                                                   |
| 3   | Does the router see only our tools?                    | Yes                          | The resolved agent lists `start_worker, reply, recall`; no built-in tools are installed                                                                                                                                                      |
| 4   | Time to first output                                   | 2.3–3.4 s for the first turn | 3 runs of the same event: 3.27, 2.33, 2.27 s; crash test runs 3.41, 2.64 s. Codex with the agent home was 2.3–2.9 s in the first proof                                                                                                       |
| 5   | Time for a whole event                                 | 6.7–10.1 s                   | Start worker → reply → closing text: three model turns of about 2–3 s each                                                                                                                                                                   |
| 6   | Does a steer join a running turn?                      | Yes, after the current tool  | Steer sent at 2.8 s while a 20 s `recall` ran; placed right after its result at 22.8 s; the next turn did both the reply and the new worker                                                                                                  |
| 7   | Does a `kill -9` during a tool call recover?           | Yes                          | Reopened storage reran `recall` (safe to repeat), then replied and finished. No model call was repeated for the step already taken                                                                                                           |
| 8   | Does a `kill -9` while the model is answering recover? | Yes                          | Reopened storage sent the model request again and finished; about 5 s slower than a clean run                                                                                                                                                |
| 9   | Is a repeated event ignored?                           | Yes                          | Submitting the same `requestId` after the restart returned the original submission (id 8) instead of a new one                                                                                                                               |
| 10  | Can a T3 worker call Physalia's MCP server?            | Yes                          | The worker's `ask` arrived 15 s after launch, with its worker ID. T3 kept the MCP server from the launch settings next to its own                                                                                                            |
| 11  | Does a worker's report reach the router?               | Yes                          | `report` arrived 30 s after `ask`, was submitted to the router as a new event, and the router replied in the thread with the commit and test results 2.8 s later                                                                             |

## Other findings

- **Workers follow what Physalia tells them, including bad advice.** The proof's `ask` answer was a fixed text that
  said "Use branch main". The worker then switched its own worktree to `main` and committed there (`c6296e2`), instead
  of its T3 branch (measured: `git worktree list` shows that worktree on `main`). The main checkout was on another
  branch, so git allowed it. So: briefs and `ask` answers must never name a branch to commit to, and the worker
  tracker should check which branch a finished worker committed to (inferred).

- **The router's closing text goes nowhere.** After its tool calls, the router ends with a short text such as
  "Worker started for sandbox-repo." (measured). Physalia should ignore it, or end the run with
  `control: { terminate: true }` from `reply`, to save one model turn of about 2–3 s (inferred).
- **The router did not always reply.** In 1 of 3 basic runs it only started the worker, with no Slack reply
  (measured). The prompt needs a firmer rule, or core should acknowledge the event itself (inferred).
- **No prompt cache hits** (`cacheRead: 0` in every run, measured). The prompts were about 1,000–1,700 tokens; OpenAI
  caches only longer prompts (guess). A real router prompt with memory will be longer.
- **Cost display.** Pi reports API prices (about $0.0015–0.003 per event); on the subscription these are not charged
  (inferred from the sign-in type).
- Node prints `ExperimentalWarning: SQLite is an experimental feature` because Pi uses `node:sqlite` (measured).

## Not tested

- Compaction over a long-lived router conversation.
- Several events arriving at once (queueing several follow-ups).
- Token refresh after the access token expires.

## How to rerun

```bash
cd ~/projects/physalia/poc/router-pi
node login.mjs            # once; open the printed link
node router.mjs basic     # or steer, crash (then kill -9 it), resume, worker
```

The `worker` test needs T3 running: `cd ~/projects/physalia/poc && ./t3 serve --port 3999 --host 127.0.0.1 --no-browser sandbox-repo`.
Logs are in `logs/`; every tool call is in `logs/tools.jsonl`.
