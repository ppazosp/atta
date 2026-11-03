import { Effect, Layer, Option } from "effect"
import { EventId, OperationId, type WorkerId } from "./domain/ids.ts"
import { parseThreadKey } from "./domain/thread.ts"
import { Memory } from "../ports/Memory.ts"
import { Store } from "../ports/Store.ts"
import { WorkerApi, WorkerApiError } from "../ports/WorkerApi.ts"
import { now } from "./clock.ts"
import { Dispatch } from "./Dispatch.ts"
import { Intake } from "./Intake.ts"
import { Outbox } from "./Outbox.ts"

/** The longest message a worker may post. Its full result goes through the router; `say` is for short answers. */
export const MAX_SAY = 3000

/** What workers can ask of atta. Workers have no memory; this is where they reach atta's. */
export const WorkerApiLive = Layer.effect(
  WorkerApi,
  Effect.gen(function* () {
    const store = yield* Store
    const memory = yield* Memory
    const intake = yield* Intake
    const dispatch = yield* Dispatch
    const outbox = yield* Outbox

    const known = (workerId: WorkerId) =>
      store.workers.get(workerId).pipe(
        Effect.mapError((e) => new WorkerApiError({ reason: "failed", message: e.message })),
        Effect.flatMap((found) =>
          Option.match(found, {
            onNone: () =>
              Effect.fail(new WorkerApiError({ reason: "unknown_worker", message: `no worker ${workerId}` })),
            onSome: (worker) =>
              worker.state === "retired"
                ? Effect.fail(
                    new WorkerApiError({ reason: "worker_retired", message: `${workerId} is retired` }),
                  )
                : Effect.succeed(worker),
          }),
        ),
      )

    return WorkerApi.of({
      ask: ({ workerId, question }) =>
        Effect.gen(function* () {
          yield* known(workerId)
          const found = yield* memory
            .recall(question)
            .pipe(Effect.mapError((e) => new WorkerApiError({ reason: "failed", message: e.message })))
          return found === ""
            ? "atta has no more context than your brief. Make the most reasonable choice, and say which in your report."
            : found
        }),
      report: ({ workerId, text }) =>
        Effect.gen(function* () {
          yield* known(workerId)
          const at = yield* now
          const id = EventId.make(`report:${workerId}:${at}:${crypto.randomUUID()}`)
          const routed = yield* intake
            .accept({
              _tag: "WorkerReport",
              id,
              source: "worker",
              externalId: id,
              receivedAt: at,
              workerId,
              text,
            })
            .pipe(Effect.mapError((e) => new WorkerApiError({ reason: "failed", message: e.message })))
          if (routed !== null) yield* dispatch.deliver(id)
        }),
      say: ({ workerId, text }) =>
        Effect.gen(function* () {
          if (text.length > MAX_SAY) {
            return yield* new WorkerApiError({
              reason: "failed",
              message: `say is for short messages (up to ${MAX_SAY} characters); this one has ${text.length}`,
            })
          }
          const worker = yield* known(workerId)
          // Only a worker in the middle of a run talks; a call for an idle or stopping one is not from its run.
          if (worker.state !== "running") {
            return yield* new WorkerApiError({
              reason: "failed",
              message: `${workerId} is ${worker.state}, not running`,
            })
          }
          const operationId = OperationId.make(`say:${workerId}:${crypto.randomUUID()}`)
          const target = { thread: parseThreadKey(worker.origin) }
          yield* store.transaction(outbox.enqueue(operationId, "slack_reply", { target, text })).pipe(
            Effect.andThen(outbox.run(operationId)),
            Effect.mapError((e) => new WorkerApiError({ reason: "failed", message: e.message })),
          )
        }),
    })
  }),
)
