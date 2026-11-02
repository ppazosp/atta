import { Schema } from "effect"
import { RunId, RuntimeRef, Timestamp, WorkerId } from "./ids.ts"
import { MessageRef, ThreadKey } from "./thread.ts"

/** The states of the worker statechart (docs/worker-lifecycle.png). */
export const WorkerState = Schema.Literals([
  "launching",
  "running",
  "idle",
  "interrupted",
  "failed",
  "stopping",
  "retired",
])
export type WorkerState = typeof WorkerState.Type

/** How a worker run ended, as shown in the plan message and told to the router. */
export const RunOutcome = Schema.Literals(["completed", "failed", "interrupted", "stopped"])
export type RunOutcome = typeof RunOutcome.Type

/** What the router asks a worker to do. The worker manager appends atta's fixed footer before launch. */
export const Brief = Schema.Struct({
  repo: Schema.NonEmptyString,
  title: Schema.NonEmptyString,
  text: Schema.NonEmptyString,
})
export type Brief = typeof Brief.Type

export const Worker = Schema.Struct({
  id: WorkerId,
  brief: Brief,
  /** The thread the worker was started from. It is also subscribed to it. */
  origin: ThreadKey,
  state: WorkerState,
  runtimeRef: Schema.NullOr(RuntimeRef),
  runId: Schema.NullOr(RunId),
  /** The branch the runtime created for the worker. Checked against where it committed. */
  branch: Schema.NullOr(Schema.String),
  worktreePath: Schema.NullOr(Schema.String),
  /** Where the tracker continues reading steps. The runtime returns no next position on its last page. */
  stepPosition: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  planMessage: Schema.NullOr(MessageRef),
  createdAt: Timestamp,
  updatedAt: Timestamp,
})
export type Worker = typeof Worker.Type

/** One step of a worker run, read from the runtime, shown as one task in the plan message. */
export const StepKind = Schema.Literals([
  "command",
  "file_change",
  "message",
  "reasoning",
  "checkpoint",
  /** One step of the worker's own plan (Codex's plan tool), updated as the worker goes. */
  "plan_step",
  "other",
])
export type StepKind = typeof StepKind.Type

export const StepStatus = Schema.Literals(["pending", "in_progress", "complete", "error"])
export type StepStatus = typeof StepStatus.Type

export const Step = Schema.Struct({
  /** Stable across reads, so a step read again (still running, then finished) updates the same plan task. */
  id: Schema.NonEmptyString,
  position: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  runId: RunId,
  kind: StepKind,
  title: Schema.String,
  status: StepStatus,
  detail: Schema.NullOr(Schema.String),
})
export type Step = typeof Step.Type
