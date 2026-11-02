import { Context, Effect, Layer } from "effect"
import type { Event } from "./domain/event.ts"
import { OperationId } from "./domain/ids.ts"
import { route, type Route } from "./domain/routing.ts"
import { threadKey } from "./domain/thread.ts"
import { Store, type StoreError } from "../ports/Store.ts"
import { now } from "./clock.ts"
import { Outbox } from "./Outbox.ts"
import { Settings } from "./Settings.ts"

export const reactionOp = (eventId: string, step: "seen" | "done-seen" | "done") =>
  OperationId.make(`react:${eventId}:${step}`)

/**
 * Takes every event in: stores it once, routes it and records where it goes, all in one transaction. A Slack
 * message that is not dropped also gets 👀 queued in the same transaction. Returns the route, or null when the
 * event was already stored.
 */
export class Intake extends Context.Service<
  Intake,
  {
    accept(event: Event): Effect.Effect<Route | null, StoreError>
    /** Routes an event that was stored but not routed (recovery after a crash). */
    redispatch(event: Event): Effect.Effect<Route, StoreError>
  }
>()("atta/core/Intake") {
  static readonly layer = Layer.effect(
    Intake,
    Effect.gen(function* () {
      const store = yield* Store
      const outbox = yield* Outbox
      const settings = yield* Settings

      const dispatch = Effect.fn("Intake.dispatch")(function* (event: Event) {
        const at = yield* now
        const subscribers =
          event._tag === "UserMessage" ? yield* store.subscriptions.subscribers(threadKey(event.thread)) : []
        const decided = route(event, { trustedUserId: settings.slack.trustedUserId, subscribers })
        switch (decided._tag) {
          case "Drop":
            yield* Effect.logInfo(`dropped event ${event.id}: ${decided.reason}`)
            break
          case "ToRouter":
            yield* store.deliveries.record(event.id, "router", at)
            break
          case "ToWorker":
            yield* store.deliveries.record(event.id, `worker:${decided.workerId}`, at)
            break
          case "StopWorkers":
            for (const id of decided.workerIds) yield* store.deliveries.record(event.id, `worker:${id}`, at)
            break
        }
        if (event._tag === "UserMessage" && decided._tag !== "Drop") {
          yield* outbox.enqueue(reactionOp(event.id, "seen"), "slack_react", {
            message: event.message,
            emoji: "eyes",
          })
        }
        yield* store.events.markDispatched(event.id, at)
        return decided
      })

      return Intake.of({
        accept: (event) =>
          store.transaction(
            Effect.gen(function* () {
              const { inserted } = yield* store.events.insert(event)
              if (!inserted) return null
              if (event._tag === "UserMessage")
                yield* store.threads.touch(threadKey(event.thread), event.receivedAt)
              return yield* dispatch(event)
            }),
          ),
        redispatch: (event) => store.transaction(dispatch(event)),
      })
    }),
  )
}
