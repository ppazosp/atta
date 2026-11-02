import type { Effect } from "effect"
import { Context, Schema } from "effect"
import type { OperationId, RunId, RuntimeRef } from "../core/domain/ids.ts"
import type { Brief, RunOutcome, Step } from "../core/domain/worker.ts"

export class RuntimeError extends Schema.TaggedError<RuntimeError>()("RuntimeError", {
  operation: Schema.String,
  message: Schema.String,
}) {}

export interface Launched {
  readonly runtimeRef: RuntimeRef
  readonly runId: RunId
  /** The branch and worktree the runtime created for the worker, when it reports them at launch. */
  readonly branch: string | null
  readonly worktreePath: string | null
}

export interface RunStatus {
  readonly runId: RunId
  readonly state: "running" | "finished"
  /** Set when `state` is finished. A run cut off by a runtime restart is `interrupted`. */
  readonly outcome: RunOutcome | null
  readonly finalMessage: string | null
  /** Where the worker's worktree is now, read from git, so a worker that switched branches is caught. */
  readonly currentBranch: string | null
  readonly headCommit: string | null
}

/** Where workers run (T3 Code in v0). */
export class Runtime extends Context.Service<
  Runtime,
  {
    /** Starts a worker in its own worktree. A repeated `operationId` returns the same worker. */
    launch(input: {
      readonly repo: string
      readonly brief: Brief
      readonly operationId: OperationId
    }): Effect.Effect<Launched, RuntimeError>
    /** Steers the running run, or queues a new run. Returns the run the message went to. */
    send(input: {
      readonly worker: RuntimeRef
      readonly text: string
      readonly mode: "steer" | "queue"
      readonly operationId: OperationId
    }): Effect.Effect<{ readonly runId: RunId }, RuntimeError>
    stop(input: {
      readonly worker: RuntimeRef
      readonly operationId: OperationId
    }): Effect.Effect<void, RuntimeError>
    /**
     * Steps after `afterPosition`, oldest first. `nextPosition` is where to read next; it equals `afterPosition`
     * when there is nothing new.
     */
    steps(input: {
      readonly worker: RuntimeRef
      readonly afterPosition: number
    }): Effect.Effect<{ readonly steps: ReadonlyArray<Step>; readonly nextPosition: number }, RuntimeError>
    /** The state of one run atta started, ignoring runs the runtime started on its own. */
    status(input: {
      readonly worker: RuntimeRef
      readonly runId: RunId
    }): Effect.Effect<RunStatus, RuntimeError>
  }
>()("atta/ports/Runtime") {}
