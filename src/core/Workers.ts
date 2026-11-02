import { Context, Effect, Layer, Option, Result, Schema } from "effect"
import { OperationId, WorkerId } from "./domain/ids.ts"
import { transition, type WorkerInput } from "./domain/lifecycle.ts"
import type { ThreadKey } from "./domain/thread.ts"
import type { Brief, Worker } from "./domain/worker.ts"
import { Store, StoreError } from "../ports/Store.ts"
import { now } from "./clock.ts"
import { Outbox, Results, type OutboxFailure } from "./Outbox.ts"

export class WorkerUnavailable extends Schema.TaggedError<WorkerUnavailable>()("WorkerUnavailable", {
  workerId: Schema.String,
  state: Schema.String,
  message: Schema.String,
}) {}

/** Appended to every brief. The rules here come from the proofs (a worker followed "use main" and committed there). */
const briefFooter = (workerId: WorkerId) =>
  [
    "",
    "---",
    `You are atta worker ${workerId}. atta's MCP server ("atta") is available to you.`,
    `- When you need context you do not have, call atta.ask with worker_id "${workerId}".`,
    `- When you finish, or if you are blocked, call atta.report with worker_id "${workerId}" and a short result.`,
    "- Work on the branch created for you. Do not switch branches and do not commit to main.",
  ].join("\n")

const launchOp = (workerId: WorkerId) => OperationId.make(`launch:${workerId}`)

/** Starts, messages and stops workers, keeping their state on the statechart. */
export class Workers extends Context.Service<
  Workers,
  {
    /**
     * Records a new worker and launches it. `seed` makes it idempotent: the same seed (a router tool call ID)
     * returns the same worker, so a replayed call never starts a second one.
     */
    start(input: {
      readonly brief: Brief
      readonly origin: ThreadKey
      readonly seed: string
    }): Effect.Effect<Worker, OutboxFailure | WorkerUnavailable>
    /** Runs a pending launch (also after a crash) and records its result. A failed launch leaves it `failed`. */
    launch(workerId: WorkerId): Effect.Effect<Worker, OutboxFailure | WorkerUnavailable>
    /**
     * Sends a message: a steer while running, a new run while idle, a continue while interrupted, a retry when
     * failed. Anything else is refused.
     */
    message(input: {
      readonly workerId: WorkerId
      readonly text: string
      readonly operationId: OperationId
    }): Effect.Effect<Worker, OutboxFailure | WorkerUnavailable>
    stop(input: {
      readonly workerId: WorkerId
      readonly operationId: OperationId
    }): Effect.Effect<Worker, OutboxFailure | WorkerUnavailable>
    /** Applies an input to a worker's state, with the changes to its record, in the current transaction. */
    move(
      worker: Worker,
      input: WorkerInput,
      patch?: Partial<Worker>,
      reason?: string,
    ): Effect.Effect<Worker, StoreError | WorkerUnavailable>
    get(workerId: WorkerId): Effect.Effect<Worker, StoreError | WorkerUnavailable>
  }
>()("atta/core/Workers") {
  static readonly layer = Layer.effect(
    Workers,
    Effect.gen(function* () {
      const store = yield* Store
      const outbox = yield* Outbox

      const get = Effect.fn("Workers.get")(function* (workerId: WorkerId) {
        const found = yield* store.workers.get(workerId)
        if (Option.isNone(found)) {
          return yield* new WorkerUnavailable({
            workerId,
            state: "unknown",
            message: `no worker ${workerId}`,
          })
        }
        return found.value
      })

      const move = Effect.fn("Workers.move")(function* (
        worker: Worker,
        input: WorkerInput,
        patch: Partial<Worker> = {},
        reason?: string,
      ) {
        const to = transition(worker.state, input)
        if (Result.isFailure(to)) {
          return yield* new WorkerUnavailable({
            workerId: worker.id,
            state: worker.state,
            message: `cannot ${input} a worker that is ${worker.state}`,
          })
        }
        const at = yield* now
        const next: Worker = { ...worker, ...patch, state: to.success, updatedAt: at }
        yield* store.transitions.append({
          workerId: worker.id,
          from: worker.state,
          to: to.success,
          input,
          reason: reason ?? null,
          at,
        })
        yield* store.workers.update(next)
        if (to.success === "retired") {
          for (const thread of yield* store.subscriptions.threadsOf(worker.id)) {
            yield* store.subscriptions.unsubscribe(thread, worker.id)
          }
        }
        return next
      })

      const launch = Effect.fn("Workers.launch")(function* (workerId: WorkerId) {
        const worker = yield* get(workerId)
        if (worker.state !== "launching") return worker
        const result = yield* outbox.run(launchOp(workerId)).pipe(
          Effect.flatMap((raw) => Schema.decodeUnknownEffect(Results.worker_launch)(raw)),
          Effect.result,
        )
        if (Result.isSuccess(result)) {
          const launched = result.success
          return yield* store.transaction(
            move(worker, "launched", {
              runtimeRef: launched.runtimeRef,
              runId: launched.runId,
              branch: launched.branch,
              worktreePath: launched.worktreePath,
            }),
          )
        }
        // The caller tells the router: start_worker returns the failure, and recovery reports it as an event.
        return yield* store.transaction(move(worker, "launch_failed", {}, String(result.failure)))
      })

      const start = Effect.fn("Workers.start")(function* (input: {
        readonly brief: Brief
        readonly origin: ThreadKey
        readonly seed: string
      }) {
        const workerId = WorkerId.make(`w-${input.seed}`)
        yield* store.transaction(
          Effect.gen(function* () {
            if (Option.isSome(yield* store.workers.get(workerId))) return
            const at = yield* now
            yield* store.workers.insert({
              id: workerId,
              brief: input.brief,
              origin: input.origin,
              state: "launching",
              runtimeRef: null,
              runId: null,
              branch: null,
              worktreePath: null,
              stepPosition: 0,
              planMessage: null,
              createdAt: at,
              updatedAt: at,
            })
            yield* store.subscriptions.subscribe(input.origin, workerId, at)
            yield* outbox.enqueue(launchOp(workerId), "worker_launch", {
              repo: input.brief.repo,
              brief: { ...input.brief, text: input.brief.text + briefFooter(workerId) },
            })
          }),
        )
        return yield* launch(workerId)
      })

      const send = Effect.fn("Workers.send")(function* (
        worker: Worker,
        input: WorkerInput,
        text: string,
        mode: "steer" | "queue",
        operationId: OperationId,
      ) {
        if (worker.runtimeRef === null) {
          return yield* new WorkerUnavailable({
            workerId: worker.id,
            state: worker.state,
            message: "not launched",
          })
        }
        const runtimeRef = worker.runtimeRef
        yield* store.transaction(
          outbox.enqueue(operationId, "worker_send", { worker: runtimeRef, text, mode }),
        )
        const sent = yield* outbox.run(operationId).pipe(
          Effect.flatMap((raw) => Schema.decodeUnknownEffect(Results.worker_send)(raw)),
          Effect.mapError((e) =>
            e._tag === "SchemaError" ? new StoreError({ operation: "worker_send", message: e.message }) : e,
          ),
        )
        // A new run gets its own plan message; a steer joins the current run and its plan.
        const patch = mode === "queue" ? { runId: sent.runId, planMessage: null } : { runId: sent.runId }
        return yield* store.transaction(move(worker, input, patch))
      })

      const message = Effect.fn("Workers.message")(function* (input: {
        readonly workerId: WorkerId
        readonly text: string
        readonly operationId: OperationId
      }) {
        const worker = yield* get(input.workerId)
        switch (worker.state) {
          case "running":
            return yield* send(worker, "steered", input.text, "steer", input.operationId)
          case "idle":
            return yield* send(worker, "follow_up", input.text, "queue", input.operationId)
          case "interrupted":
            return yield* send(worker, "continue", input.text, "queue", input.operationId)
          case "failed":
            return yield* send(worker, "retry", input.text, "queue", input.operationId)
          default:
            return yield* new WorkerUnavailable({
              workerId: worker.id,
              state: worker.state,
              message: `a ${worker.state} worker cannot take messages`,
            })
        }
      })

      const stop = Effect.fn("Workers.stop")(function* (input: {
        readonly workerId: WorkerId
        readonly operationId: OperationId
      }) {
        const worker = yield* get(input.workerId)
        if (worker.state !== "running" || worker.runtimeRef === null) {
          return yield* new WorkerUnavailable({
            workerId: worker.id,
            state: worker.state,
            message: `only a running worker can be stopped`,
          })
        }
        const runtimeRef = worker.runtimeRef
        const stopping = yield* store.transaction(
          Effect.gen(function* () {
            yield* outbox.enqueue(input.operationId, "worker_stop", { worker: runtimeRef })
            return yield* move(worker, "stop_requested")
          }),
        )
        yield* outbox.run(input.operationId)
        return stopping
      })

      return Workers.of({ start, launch, message, stop, move, get })
    }),
  )
}
