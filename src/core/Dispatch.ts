import { Context, Effect, Layer, Option } from "effect"
import type { Event } from "./domain/event.ts"
import { OperationId, WorkerId, type EventId } from "./domain/ids.ts"
import { isStopCommand } from "./domain/routing.ts"
import { RouterEngine } from "../ports/RouterEngine.ts"
import { Store, type DeliveryTarget } from "../ports/Store.ts"
import { reactionOp } from "./Intake.ts"
import { Outbox } from "./Outbox.ts"
import { renderEvent } from "./render.ts"
import { Workers } from "./Workers.ts"

/**
 * Delivers routed events: to the router as an input, or straight to a worker. When the event is a Slack message,
 * 👀 becomes ✅ once it is handled: when the router has answered it, or when the worker has taken it.
 */
export class Dispatch extends Context.Service<
  Dispatch,
  {
    deliver(eventId: EventId): Effect.Effect<void>
    /** Delivers everything still pending. Used once at startup. */
    readonly recover: Effect.Effect<void>
  }
>()("atta/core/Dispatch") {
  static readonly layer = Layer.effect(
    Dispatch,
    Effect.gen(function* () {
      const store = yield* Store
      const outbox = yield* Outbox
      const engine = yield* RouterEngine
      const workers = yield* Workers
      // Waits for the router's answers outlive a single delivery; they belong to the service's scope.
      const scope = yield* Effect.scope

      const markHandled = Effect.fn("Dispatch.markHandled")(function* (
        event: Event,
        outcome: "done" | "failed",
      ) {
        if (event._tag !== "UserMessage") return
        const unreact = reactionOp(event.id, "done-seen")
        const react = reactionOp(event.id, "done")
        yield* store.transaction(
          Effect.gen(function* () {
            yield* outbox.enqueue(unreact, "slack_unreact", { message: event.message, emoji: "eyes" })
            yield* outbox.enqueue(react, "slack_react", {
              message: event.message,
              emoji: outcome === "done" ? "white_check_mark" : "warning",
            })
          }),
        )
        yield* outbox.run(unreact)
        yield* outbox.run(react)
      })

      const toRouter = Effect.fn("Dispatch.toRouter")(function* (event: Event) {
        const worker =
          event._tag === "WorkerReport" || event._tag === "WorkerFinished"
            ? Option.getOrNull(yield* store.workers.get(event.workerId))
            : null
        yield* engine.submit({ requestId: event.id, content: renderEvent(event, worker), whenBusy: "queue" })
        yield* engine.settled(event.id).pipe(
          Effect.flatMap((settled) => markHandled(event, settled.status === "answered" ? "done" : "failed")),
          Effect.catch((error) => Effect.logWarning(`could not finish event ${event.id}`, error)),
          Effect.forkIn(scope),
        )
      })

      const toWorker = Effect.fn("Dispatch.toWorker")(function* (event: Event, workerId: WorkerId) {
        if (event._tag !== "UserMessage") return yield* toRouter(event)
        const handled = isStopCommand(event.text)
          ? workers.stop({ workerId, operationId: OperationId.make(`stop:${event.id}:${workerId}`) })
          : workers.message({
              workerId,
              text: event.text,
              operationId: OperationId.make(`send:${event.id}:${workerId}`),
            })
        yield* handled.pipe(
          Effect.andThen(markHandled(event, "done")),
          // The worker changed state since routing: let the router decide instead.
          Effect.catchTag("WorkerUnavailable", () => toRouter(event)),
        )
      })

      const deliverOne = Effect.fn("Dispatch.deliverOne")(function* (event: Event, target: DeliveryTarget) {
        yield* target === "router"
          ? toRouter(event)
          : toWorker(event, WorkerId.make(target.slice("worker:".length)))
        yield* store.deliveries.markDone(event.id, target)
      })

      const deliver = Effect.fn("Dispatch.deliver")(
        function* (eventId: EventId) {
          const found = yield* store.events.get(eventId)
          if (Option.isNone(found)) return
          // 👀 first, before the router or a worker spends any time on it. Intake queued it with the event.
          if (found.value._tag === "UserMessage") {
            yield* outbox
              .run(reactionOp(eventId, "seen"))
              .pipe(Effect.catch((error) => Effect.logWarning(`could not react to ${eventId}`, error)))
          }
          const pending = (yield* store.deliveries.pending()).filter((d) => d.eventId === eventId)
          for (const delivery of pending) {
            yield* deliverOne(found.value, delivery.target).pipe(
              Effect.catch((error) =>
                Effect.logWarning(`delivery of ${eventId} to ${delivery.target} failed`, error),
              ),
            )
          }
        },
        Effect.catch((error) => Effect.logError("delivery failed", error)),
      )

      return Dispatch.of({
        deliver,
        recover: store.deliveries.pending().pipe(
          Effect.flatMap((pending) => Effect.forEach(new Set(pending.map((d) => d.eventId)), deliver)),
          Effect.catch((error) => Effect.logError("delivery recovery failed", error)),
          Effect.asVoid,
        ),
      })
    }),
  )
}
