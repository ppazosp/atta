import { execFile } from "node:child_process"
import { realpath } from "node:fs/promises"
import { promisify } from "node:util"
import { Effect, Schema } from "effect"
import { RuntimeError } from "../../ports/Runtime.ts"

const execute = promisify(execFile)
const FileError = Schema.Struct({ code: Schema.String })

export const readWorktree = Effect.fn("T3Runtime.readWorktree")(function* (path: string | null) {
  if (path === null) return { currentBranch: null, headCommit: null }
  const directory = yield* Effect.tryPromise(() => realpath(path)).pipe(
    Effect.catch((error) =>
      Schema.decodeUnknownEffect(FileError)(error.cause).pipe(
        Effect.flatMap(({ code }) => (code === "ENOENT" ? Effect.succeed(null) : Effect.fail(error))),
      ),
    ),
    Effect.mapError(
      () => new RuntimeError({ operation: "status", message: "Cannot inspect the T3 worktree." }),
    ),
  )
  if (directory === null) return { currentBranch: null, headCommit: null }
  const git = (args: ReadonlyArray<string>) =>
    Effect.tryPromise({
      try: (signal) => execute("git", ["-C", directory, ...args], { signal, timeout: 10_000 }),
      catch: () =>
        new RuntimeError({
          operation: "status",
          message: "Cannot read the branch and HEAD from the T3 worktree.",
        }),
    }).pipe(Effect.map(({ stdout }) => stdout.trim()))
  const root = yield* git(["rev-parse", "--show-toplevel"])
  const resolvedRoot = yield* Effect.tryPromise(() => realpath(root)).pipe(
    Effect.mapError(
      () => new RuntimeError({ operation: "status", message: "Cannot resolve the T3 worktree root." }),
    ),
  )
  // Without this check, git can quietly read a parent checkout after worktree metadata is lost.
  if (resolvedRoot !== directory)
    return yield* new RuntimeError({
      operation: "status",
      message: "The T3 worktree path is not a Git worktree root.",
    })
  const branch = yield* git(["rev-parse", "--abbrev-ref", "HEAD"])
  const headCommit = yield* git(["rev-parse", "--verify", "HEAD"])
  return { currentBranch: branch === "HEAD" ? null : branch, headCommit }
})
