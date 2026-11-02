import { Effect, Layer, Schedule, Stream } from "effect"
import { EventId, RunId } from "./domain/ids.ts"
import type { Event } from "./domain/event.ts"
import { Channel } from "../ports/Channel.ts"
import { ScheduledPrompts } from "../ports/ScheduledPrompts.ts"
import { Store } from "../ports/Store.ts"
import { now } from "./clock.ts"
import { Dispatch } from "./Dispatch.ts"
import { Intake } from "./Intake.ts"
import { Outbox } from "./Outbox.ts"
import { RouterToolsLive } from "./RouterToolsLive.ts"
import { TRACKER_INTERVAL_MS } from "./Settings.ts"
import { Tracker } from "./Tracker.ts"
import { WorkerApiLive } from "./WorkerApiLive.ts"
import { Workers } from "./Workers.ts"

/**
 * Core in two layers, because the router engine sits between them: it needs `RouterTools` from the first, and
 * dispatch in the second needs the engine.
 *
 * `CoreBaseLayer` needs the ports (Store, Channel, Runtime, Memory) and Settings, and provides Outbox, Intake,
 * Workers and RouterTools. The app (WP8) builds the engine (WP4) on top of it, then `CoreLayer`.
 */
export const CoreBaseLayer = RouterToolsLive.pipe(
  Layer.provideMerge(Workers.layer),
  Layer.provideMerge(Intake.layer),
  Layer.provideMerge(Outbox.layer),
)

/** Needs `CoreBaseLayer`'s services and the RouterEngine; provides Dispatch, Tracker and WorkerApi (for WP7). */
export const CoreLayer = Layer.mergeAll(Tracker.layer, WorkerApiLive).pipe(Layer.provideMerge(Dispatch.layer))

/** Finishes what a crash cut short: outbox actions (with their results), undispatched events and deliveries. */
export const recover = Effect.gen(function* () {
  const store = yield* Store
  const outbox = yield* Outbox
  const workers = yield* Workers
  const intake = yield* Intake
  const dispatch = yield* Dispatch

  yield* outbox.recover(workers.recoveryHandlers)
  // A launch that failed while atta was down, or during recovery, still has to reach the router once.
  for (const worker of yield* store.workers.inStates(["failed"])) {
    if (worker.runtimeRef !== null) continue
    const id = EventId.make(`finished:${worker.id}:launch`)
    yield* intake.accept({
      _tag: "WorkerFinished",
      id,
      source: "worker",
      externalId: `${worker.id}:launch`,
      receivedAt: yield* now,
      workerId: worker.id,
      runId: RunId.make(`${worker.id}:launch`),
      outcome: "failed",
      finalMessage: "The worker could not start.",
      branch: null,
      commit: null,
      wrongBranch: false,
    })
  }
  for (const event of yield* store.events.undispatched()) yield* intake.redispatch(event)
  yield* dispatch.recover
}).pipe(Effect.catch((error) => Effect.logError("recovery failed", error)))

/** atta's main loop: recover, then take events from Slack and the clock, and follow workers. */
export const runCore = Effect.gen(function* () {
  const channel = yield* Channel
  const prompts = yield* ScheduledPrompts
  const intake = yield* Intake
  const dispatch = yield* Dispatch
  const tracker = yield* Tracker

  yield* recover
  const take = (event: Event) =>
    intake.accept(event).pipe(
      Effect.flatMap((routed) =>
        routed === null || routed._tag === "Drop" ? Effect.void : dispatch.deliver(event.id),
      ),
      Effect.catch((error) => Effect.logError(`event ${event.id} failed`, error)),
    )
  yield* Effect.all(
    [
      channel.events.pipe(Stream.runForEach(take)),
      prompts.events.pipe(Stream.runForEach(take)),
      tracker.tick.pipe(Effect.repeat(Schedule.spaced(TRACKER_INTERVAL_MS))),
    ],
    { concurrency: "unbounded", discard: true },
  )
})
