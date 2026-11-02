---
name: codex-worker
description: Run the Codex CLI (gpt-6.1-sol, xhigh) on an atta work package or a review, in its own git worktree, then check and land its work. Use when building work packages in parallel, or when asking for an independent review of a change.
---

# Codex as a worker

Codex builds one work package at a time in its own worktree, while you build another. You stay responsible for what lands: you read its diff, run the checks and commit.

## Start a work package

1. Start from the latest `main`, and record the base commit:

   ```bash
   git fetch origin && base=$(git rev-parse origin/main)
   git worktree add --no-track -b wp/<id>-<slug> ../wt/<id> "$base"
   echo "$base" > ../wt/<id>.base
   ```

2. Prepare the worktree, and stop if any step fails:

   ```bash
   cd ../wt/<id>
   pnpm install --frozen-lockfile --offline   # the store is already filled by the main checkout
   ln -s "$(git -C <main checkout> rev-parse --show-toplevel)/.repos" .repos   # read-only references
   pnpm check                                  # must pass before Codex starts
   ```

3. Write the brief to `../wt/<id>.brief.md`. A brief is self-contained. It names:
   - the work package ID and its section in `docs/v0-plan.md`
   - the files it owns, and that it must not change any other file
   - the interfaces it codes against (from `src/ports`) and the fakes and fixtures it can use
   - what done means: the tests to add and `pnpm check` passing
   - "Do not commit. Leave a summary of what you did, what you could not do, and every file you changed."

4. Run Codex in the background, with a log and the last message saved. `--ignore-user-config` leaves out your personal MCP servers and plugins, so the worker can only touch the worktree and the network:

   ```bash
   codex exec --ignore-user-config -m gpt-6.1-sol -c model_reasoning_effort="xhigh" \
     -s workspace-write -c sandbox_workspace_write.network_access=true \
     -C ../wt/<id> -o ../wt/<id>.result.md \
     "$(cat ../wt/<id>.brief.md)" > ../wt/<id>.log 2>&1
   ```

   Give the job a watchdog: wake when `../wt/<id>.result.md` appears, or when the log has not changed for about 5 minutes. Read the files; the exit status alone is not evidence.

## Land a work package

1. Read `../wt/<id>.result.md`.
2. Stage everything, new files included, and list what changed against the recorded base:

   ```bash
   git -C ../wt/<id> add -A
   git -C ../wt/<id> diff --cached --stat "$(cat ../wt/<id>.base)"
   ```

   Every path must be one the package owns. Unstage and discard anything else.

3. Read the full staged diff, and run `pnpm check` in the worktree. Fix what fails, or send Codex a follow-up with the failure.
4. Review against the plan and `AGENTS.md`: boundaries, schemas at edges, tagged errors, tests that would catch a regression.
5. Commit on the branch, with a Conventional Commits message that says what changed. Push and open a pull request.
6. Remove the worktree when the pull request merges: `git worktree remove ../wt/<id>`.

## Ask for a review

Run Codex read-only on the branch, with a prompt that gives it the code and the plan section, not your conclusion:

```bash
codex exec --ignore-user-config -m gpt-6.1-sol -c model_reasoning_effort="xhigh" -s read-only -C ../wt/<id> \
  -o ../wt/<id>.review.md \
  "Review the staged changes of this worktree (git diff --cached $(cat ../wt/<id>.base)) for correctness, edge cases, security and fit with docs/v0-plan.md and AGENTS.md. List findings, most severe first, each with file, line and a concrete failure. Say what you checked and found fine." \
  > ../wt/<id>.review.log 2>&1
```

Verify each finding before acting on it. A finding is a claim, not a fact.

## Limits

- The sandbox keeps Codex inside the worktree, but it does not know which files a package owns. The path check in step 2 of landing does.
- With networking on, Codex can reach the internet. It cannot reach your MCP servers or plugins, because of `--ignore-user-config`.
- In `workspace-write`, Codex cannot write to the main repository's `.git`, so it cannot commit from a worktree. That is intended: you commit.
- Two workers never own the same file. If two packages must change one file, sequence them.
