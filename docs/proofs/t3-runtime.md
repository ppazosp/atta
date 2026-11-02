# Physalia proof: T3 Code as the worker runtime

> Written during the proofs, when the project was called Physalia and lived outside this repository. Paths like `~/projects/physalia/poc` refer to that proof folder, which is not published.

Run on 2 November 2026 against T3 Code nightly `0.1.7-nightly.20261102.4066` and Codex CLI 0.161.0, on this Mac.
Everything below was measured unless it says "inferred" or "not tested".

## Summary

T3 works as the runtime for Physalia, unchanged.

- A router thread on `gpt-6.1-terra` (medium) started `gpt-6.1-sol` workers. They fixed a real bug, committed
  and reported back.
- Physalia can drive T3 as a signed-in outside MCP client. It can launch workers in their own worktrees, read
  their steps one at a time, and steer them while they run.
- T3's built-in scheduler can post scheduled events into the router thread.

Four findings shape the design:

1. **Start workers with `t3_thread_launch`, not `delegate_task`.** A `delegate_task` worker shares the
   project's checkout: it switched the main checkout to its own branch.
2. **Physalia needs its own completion watcher.** T3 wakes the router only when a `delegate_task` worker
   finishes its first task. It does not wake it for launched workers, for follow-up runs, or when the launch
   passes `delegatedFrom`.
3. **The watchdog must resubmit interrupted work.** After a server restart, T3 continues an interrupted run
   itself, but that run only reports "interrupted" and does not redo the work.
4. **Talk to T3 through its MCP endpoint, not the CLI.** The CLI returns final messages only and cannot
   steer. The MCP endpoint returns typed steps by position and supports steering.

## Setup

- T3 ran headless with its own data folder (`T3CODE_HOME=poc/.t3home`, 356 MB) on port 3999. `~/.t3` and
  the T3 Code desktop app (0.0.45) were not touched: no file in `~/.t3` changed during the proof.
- `poc/t3` wraps the official npm launcher. The launcher downloads the release from GitHub and checks its
  SHA-256 before running it.
- Test repository: `poc/sandbox-repo`, a small Python module with a deliberate bug.
- T3 settings: `poc/.t3home/userdata/settings.json`. T3 reloads it without a restart.
- Agent Codex home: `~/.physalia/codex-home`, with its own ChatGPT login (you signed in), and logins stored
  as files.
- Physalia's T3 sign-in: `poc/mcp-client`. Tokens are in `~/.physalia/t3-mcp`, approved with full access.

## Results

| #   | Question                                               | Answer                                                       | Evidence                                                                                                                                                                                                                                                  |
| --- | ------------------------------------------------------ | ------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Does nightly run headless in its own folder?           | Yes                                                          | `t3 serve` with `T3CODE_HOME`; `~/.t3` unchanged                                                                                                                                                                                                          |
| 2   | Can an event start a router turn that starts a worker? | Yes                                                          | Router on terra (medium) started sol workers: bug fixed, tests passed, committed                                                                                                                                                                          |
| 3a  | Does a finished worker wake the router?                | Only for `delegate_task`, first task                         | Router run started about 4 s after the worker finished, with the result                                                                                                                                                                                   |
| 3b  | Does a follow-up run wake the router?                  | No                                                           | Router run count unchanged 20 s after the follow-up finished                                                                                                                                                                                              |
| 3c  | Does a `t3_thread_launch` worker wake the router?      | No, even with `delegatedFrom`                                | Router stayed at 9 runs, 20 s after the worker finished                                                                                                                                                                                                   |
| 4   | Can a worker get its own worktree?                     | Yes, with `t3_thread_launch` (`workspaceStrategy: worktree`) | Workers ran in `.t3home/worktrees/sandbox-repo/…` on their own branches                                                                                                                                                                                   |
| 5   | Can router and worker use different reasoning levels?  | Yes, per thread                                              | Router: T3 default model setting, sent as `medium`. Worker: `modelSelection.options` in `t3_thread_launch`, sent as `high`. A per-instance `launchArgs` default also works                                                                                |
| 6   | Can the agent have its own Codex setup?                | Yes                                                          | After the switch, router logs show none of your MCP servers, no notify hook, no priority tier. Router turns fell from 13.5–15.7 s to 2.6–5.3 s. The router kept its history: T3 carried it into a new Codex session when the old one could not be resumed |
| 7   | Can Physalia read progress step by step?               | Yes, over MCP                                                | `t3_thread_read` with `view: activity` returns typed items (`command_execution`, `assistant_message`, `reasoning`, `file_change`, `checkpoint`) with positions, `nextPosition` and `hasMore`. The CLI returns final messages only                         |
| 8   | Can Physalia steer a running worker?                   | Yes, over MCP                                                | `t3_thread_send` with `mode: steer` landed during a 45 s command. The worker acted on it ("I'll include a one-line docstring") and committed                                                                                                              |
| 9   | What happens to a run when the server restarts?        | A continuation run reports the interruption                  | "The wait was interrupted before any output was captured." No processes left behind                                                                                                                                                                       |
| 10  | Can T3 schedule events?                                | Yes, into the router thread                                  | The router created a one-off `schedule_task`; it ran on time as a new router turn and replied "cron ok"                                                                                                                                                   |

## Other findings

- T3 approves outside clients as read-only by default. Physalia needs "Full access" to launch and steer
  full-access workers, because T3 does not let a client steer a thread above its own level.
- An outside client cannot create a schedule in a project that has no default model. Creating it from the
  router thread works, and its runs go straight into the router thread.
- `schedule_task` also supports signed webhooks, request filters and duplicate handling. Webhooks need T3
  Connect (remote access). Not tested.
- `t3 thread send` always queues ("A busy thread gets it as its next turn; nothing is interrupted.").
- T3 writes checkpoint refs into the repository (16 under `refs/t3/` after the first round) and keeps
  worktrees in its own data folder.
- `t3_thread_read` returns `nextPosition: null` on the last page. The reader must keep its last position,
  otherwise it starts again from the beginning (this happened in the probe script).

## Design changes

Two rows below were later replaced (see `docs/v0-plan.md`): the router now runs in Physalia on Pi Durable, so
scheduled prompts come from Physalia's own Clock channel instead of T3's `schedule_task`; and workers use your own
`~/.codex` instead of `~/.physalia/codex-home`.

| Change                                                                                             | Why                          |
| -------------------------------------------------------------------------------------------------- | ---------------------------- |
| Physalia's T3 client uses T3's MCP endpoint (OAuth, full access), with the CLI as a fallback       | Questions 7 and 8            |
| The router starts workers with `t3_thread_launch`: sol, xhigh, one worktree per task               | Questions 4 and 5, finding 1 |
| New **completion watcher**: when any worker run finishes, send a "worker done" event to the router | Questions 3b and 3c          |
| The watchdog resubmits runs that end as "interrupted"                                              | Question 9                   |
| T3's `schedule_task` replaces our own scheduler. Webhooks come later through the same tool         | Question 10                  |
| Both Codex instances use `~/.physalia/codex-home`                                                  | Question 6                   |

The diagram in `docs/architecture.png` shows the updated design.

## Not tested yet

- Webhook triggers (need T3 Connect) and PR watching.
- Several workers running at the same time.
- Slack.

## How to rerun

```bash
cd ~/projects/physalia/poc
./t3 serve --port 3999 --host 127.0.0.1 --no-browser sandbox-repo
cd mcp-client && node read.mjs <thread id>
```

Logs from this run are in `poc/logs/`.
