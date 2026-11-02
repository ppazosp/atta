---
name: codex-worker
description: Run the Codex CLI (gpt-6.1-sol, xhigh) on an atta work package or a review, in its own git worktree, then check and land its work. Use when building work packages in parallel, or when asking for an independent review of a change.
---

# Codex as a worker

Codex builds one work package at a time in its own worktree, while you build another. You stay responsible for what lands: you read its diff, run the checks and commit.

## Start a work package

1. Create a worktree and branch from `main`:

   ```bash
   git worktree add --no-track -b wp/<id>-<slug> ../wt/<id> main
   (cd ../wt/<id> && pnpm install --frozen-lockfile --offline)
   ```

2. Write the brief to `../wt/<id>.brief.md`. A brief is self-contained. It names:
   - the work package ID and its section in `docs/v0-plan.md`
   - the files it owns, and the files it must not touch
   - the interfaces it codes against (from `src/ports`)
   - what done means: the tests to add and `pnpm check` passing
   - "Do not commit. Do not edit files outside this worktree. Leave a summary of what you did and what you could not do."

3. Run Codex in the background, with a log and the last message saved:

   ```bash
   codex exec -m gpt-6.1-sol -c model_reasoning_effort="xhigh" \
     -s workspace-write -c sandbox_workspace_write.network_access=true \
     -C ../wt/<id> -o ../wt/<id>.result.md \
     "$(cat ../wt/<id>.brief.md)" > ../wt/<id>.log 2>&1
   ```

   Give the job a watchdog: wake when `../wt/<id>.result.md` appears, or when the log has not changed for about 5 minutes. Read the files; the exit status alone is not evidence.

## Land a work package

1. Read `../wt/<id>.result.md` and the full diff (`git -C ../wt/<id> diff main`).
2. Run `pnpm check` in the worktree. Fix what fails, or send Codex a follow-up with the failure.
3. Review against the plan and `AGENTS.md`: boundaries, schemas at edges, tagged errors, tests that would catch a regression.
4. Commit on the branch with a message that says what and why. Push and open a pull request.
5. Remove the worktree when the pull request merges: `git worktree remove ../wt/<id>`.

## Ask for a review

Run Codex read-only on the branch, with a prompt that gives it the code and the plan section, not your conclusion:

```bash
codex exec -m gpt-6.1-sol -c model_reasoning_effort="xhigh" -s read-only -C ../wt/<id> \
  -o ../wt/<id>.review.md \
  "Review the diff of this branch against main for correctness, edge cases, security and fit with docs/v0-plan.md and AGENTS.md. List findings, most severe first, each with file, line and a concrete failure. Say what you checked and found fine." \
  > ../wt/<id>.review.log 2>&1
```

Verify each finding before acting on it. A finding is a claim, not a fact.

## Limits

- Codex uses your own `~/.codex` setup, including its MCP servers and plugins.
- In `workspace-write`, Codex cannot write to the main repository's `.git`, so it cannot commit from a worktree. That is intended: you commit.
- Two workers never own the same file. If two packages must change one file, sequence them.
