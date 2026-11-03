import type { Effect } from "effect"
import { Context, Schema } from "effect"
import type { WorkerId } from "../core/domain/ids.ts"

export class WorkerApiError extends Schema.TaggedError<WorkerApiError>()("WorkerApiError", {
  reason: Schema.Literals(["unknown_worker", "worker_retired", "failed"]),
  message: Schema.String,
}) {}

/** What workers can ask of atta, through atta's MCP server. Core implements it (WP5); WP7 exposes it. */
export class WorkerApi extends Context.Service<
  WorkerApi,
  {
    /** Answers from state and memory. Read-only. */
    ask(input: {
      readonly workerId: WorkerId
      readonly question: string
    }): Effect.Effect<string, WorkerApiError>
    /** Becomes a `WorkerReport` event for the router. */
    report(input: { readonly workerId: WorkerId; readonly text: string }): Effect.Effect<void, WorkerApiError>
    /** Posts `text` in the worker's own Slack thread, so it can answer the user while it works. */
    say(input: { readonly workerId: WorkerId; readonly text: string }): Effect.Effect<void, WorkerApiError>
  }
>()("atta/ports/WorkerApi") {}
