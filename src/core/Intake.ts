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
      const watchedChannels = new Set(settings.watchedChannels ?? [])

      const dispatch = Effect.fn("Intake.dispatch")(function* (event: Event) {
        const at = yield* now
        const key = event._tag === "UserMessage" ? threadKey(event.thread) : null
        const subscribers = key === null ? [] : yield* store.subscriptions.subscribers(key)
        const threadKnown = key === null ? true : yield* store.threads.exists(key)
        const watched = event._tag === "UserMessage" && watchedChannels.has(event.thread.conversation)
        const decided = route(event, {
          trustedUserId: settings.slack.trustedUserId,
          subscribers,
          threadKnown,
          watched,
        })
        // DMs and mentions open a thread atta may post in; a schedule's thread was recorded at intake. Watched
        // channel threads stay closed until the trusted user approves work there.
        const opensThread = decided._tag !== "Drop" && decided._tag !== "Observe"
        if (key !== null && opensThread) yield* store.threads.touch(key, at)
        switch (decided._tag) {
          case "Drop":
            yield* Effect.logInfo(`dropped event ${event.id}: ${decided.reason}`)
            break
          case "Record":
            break
          case "ToRouter":
            yield* store.deliveries.record(event.id, "router", at)
            break
          case "Observe":
            yield* store.deliveries.record(event.id, "observe", at)
            break
          case "Decide":
            // Core starts the work; then the router hears what came of its proposal, so it remembers.
            yield* store.deliveries.record(event.id, "core", at)
            yield* store.deliveries.record(event.id, "router", at)
            break
          case "Answer":
            yield* store.deliveries.record(event.id, "core", at)
            break
          case "ToWorker":
            yield* store.deliveries.record(event.id, `worker:${decided.workerId}`, at)
            break
          case "StopWorkers":
            for (const id of decided.workerIds) yield* store.deliveries.record(event.id, `worker:${id}`, at)
            break
        }
        // 👀 only where the user talks to atta; a watched channel never sees atta react.
        if (event._tag === "UserMessage" && opensThread) {
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
              if (event._tag === "ScheduledPrompt" && "thread" in event.target) {
                yield* store.threads.touch(threadKey(event.target.thread), event.receivedAt)
              }
              return yield* dispatch(event)
            }),
          ),
        redispatch: (event) => store.transaction(dispatch(event)),
      })
    }),
  )
}
