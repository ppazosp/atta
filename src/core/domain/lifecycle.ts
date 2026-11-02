import { Result, Schema } from "effect"
import type { WorkerState } from "./worker.ts"

/**
 * What can happen to a worker. Each arrow in docs/worker-lifecycle.png is one (state, input) pair below.
 * Inputs are plain tags: the facts they carry (run IDs, reasons) are stored with the transition, not here.
 */
export const WorkerInput = Schema.Literals([
  "launched",
  "launch_failed",
  "step",
  "steered",
  "reported",
  "run_completed",
  "run_interrupted",
  "run_failed",
  "stop_requested",
  "stopped",
  "follow_up",
  "continue",
  "retry",
  "drop",
  "give_up",
  "idle_expired",
])
export type WorkerInput = typeof WorkerInput.Type

export class InvalidTransition extends Schema.TaggedError<InvalidTransition>()("InvalidTransition", {
  from: Schema.String,
  input: Schema.String,
}) {}

const arrows: { readonly [S in WorkerState]: { readonly [I in WorkerInput]?: WorkerState } } = {
  launching: { launched: "running", launch_failed: "failed" },
  running: {
    step: "running",
    steered: "running",
    reported: "running",
    run_completed: "idle",
    run_interrupted: "interrupted",
    run_failed: "failed",
    stop_requested: "stopping",
  },
  idle: { follow_up: "running", idle_expired: "retired" },
  interrupted: { continue: "running", drop: "retired" },
  failed: { retry: "running", give_up: "retired" },
  stopping: { stopped: "retired" },
  retired: {},
}

/** The worker statechart as a pure function. Anything not drawn in the statechart is an error. */
export const transition = (
  from: WorkerState,
  input: WorkerInput,
): Result.Result<WorkerState, InvalidTransition> => {
  const to = arrows[from][input]
  return to === undefined ? Result.fail(new InvalidTransition({ from, input })) : Result.succeed(to)
}

/** Every arrow of the statechart, for tests and diagrams. */
export const allArrows = (): ReadonlyArray<readonly [WorkerState, WorkerInput, WorkerState]> =>
  (Object.keys(arrows) as Array<WorkerState>).flatMap((from) =>
    Object.entries(arrows[from]).map(
      ([input, to]) => [from, input as WorkerInput, to as WorkerState] as const,
    ),
  )
