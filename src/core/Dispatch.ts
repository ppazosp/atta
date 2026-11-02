import { Context, Effect, Layer, Option, Schema } from "effect"
import type { Event } from "./domain/event.ts"
import { OperationId, WorkerId, type EventId } from "./domain/ids.ts"
import { isStopCommand } from "./domain/routing.ts"
import { threadKey, type ThreadKey } from "./domain/thread.ts"
import { ObserverEngine, RouterEngine } from "../ports/RouterEngine.ts"
import { Store, type DeliveryTarget } from "../ports/Store.ts"
import { reactionOp } from "./Intake.ts"
import { Outbox, Results } from "./Outbox.ts"
import { now } from "./clock.ts"
import { renderEvent, renderObservation } from "./render.ts"
import { Workers } from "./Workers.ts"

/**
 * Delivers routed events: to the router as an input, or straight to a worker. When the event is a Slack message,
 * 👀 becomes ✅ once it is handled: when the router has answered it, or when the worker has taken it.
 */
/** The outbox operation that sends the approval request for a proposal. */
export const approvalOp = (proposalId: EventId) => OperationId.make(`approval:${proposalId}`)

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
      const observer = yield* ObserverEngine
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
        // A scheduled prompt for a new thread opens the thread first, so the router can reply and start workers there.
        let opened: ThreadKey | null = null
        if (event._tag === "ScheduledPrompt" && "conversation" in event.target) {
          const op = OperationId.make(`schedule-root:${event.id}`)
          yield* store.transaction(
            outbox.enqueue(op, "slack_reply", {
              target: { conversation: event.target.conversation },
              text: `Scheduled: ${event.schedule}`,
            }),
          )
          const root = yield* outbox
            .run(op)
            .pipe(Effect.flatMap(Schema.decodeUnknownEffect(Results.slack_reply)))
          opened = threadKey({ team: root.team, conversation: root.conversation, rootTs: root.ts })
          yield* store.threads.touch(opened, event.receivedAt)
        }
        yield* engine.submit({
          requestId: event.id,
          content: renderEvent(event, worker, opened),
          whenBusy: "queue",
        })
        yield* engine.settled(event.id).pipe(
          Effect.flatMap((settled) => markHandled(event, settled.status === "answered" ? "done" : "failed")),
          Effect.catch((error) => Effect.logWarning(`could not finish event ${event.id}`, error)),
          Effect.forkIn(scope),
        )
      })

      /**
       * A watched-channel message goes to the observer, never to the router: its text comes from anyone, and the
       * observer's tools can only propose work. Nothing is posted and nothing reacts in the channel.
       */
      const observe = Effect.fn("Dispatch.observe")(function* (event: Event) {
        if (event._tag !== "UserMessage") return
        yield* observer.submit({ requestId: event.id, content: renderObservation(event), whenBusy: "queue" })
      })

      /**
       * The user's Start or Skip on a proposal. Start opens the channel thread and starts the worker there; either
       * way the approval message loses its buttons. The worker ID comes from the proposal, so a replay starts no
       * second worker.
       */
      const decide = Effect.fn("Dispatch.decide")(function* (event: Event) {
        if (event._tag !== "ApprovalDecision") return
        const found = yield* store.events.get(event.proposalId)
        if (Option.isNone(found) || found.value._tag !== "WorkProposed") {
          return yield* Effect.logWarning(`decision ${event.id} names no proposal`)
        }
        const proposal = found.value
        let text = `Skipped: ${proposal.title}`
        if (event.decision === "approve") {
          const origin = threadKey(proposal.thread)
          yield* store.threads.touch(origin, yield* now)
          const seed = proposal.id.replace(/[^A-Za-z0-9_-]/g, "-")
          yield* workers
            .start({
              brief: { repo: proposal.repo, title: proposal.title, text: proposal.brief },
              origin,
              seed,
            })
            .pipe(
              Effect.catch((error) => Effect.logWarning(`starting proposal ${proposal.id} failed`, error)),
            )
          // A launch that has not landed yet is retried by the tracker, so only a recorded failure counts.
          const worker = yield* store.workers.get(WorkerId.make(`w-${seed}`))
          text =
            Option.isSome(worker) && worker.value.state !== "failed"
              ? `Started: ${proposal.title}`
              : `Could not start: ${proposal.title}`
        }
        const request = yield* outbox
          .run(approvalOp(proposal.id))
          .pipe(Effect.flatMap(Schema.decodeUnknownEffect(Results.approval_request)))
        const close = OperationId.make(`approval-close:${proposal.id}`)
        yield* store.transaction(outbox.enqueue(close, "approval_close", { message: request, text }))
        yield* outbox.run(close)
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
        if (target === "router") yield* toRouter(event)
        else if (target === "observe") yield* observe(event)
        else if (target === "core") yield* decide(event)
        else yield* toWorker(event, WorkerId.make(target.slice("worker:".length)))
        yield* store.deliveries.markDone(event.id, target)
      })

      const deliver = Effect.fn("Dispatch.deliver")(
        function* (eventId: EventId) {
          const found = yield* store.events.get(eventId)
          if (Option.isNone(found)) return
          const pending = (yield* store.deliveries.pending()).filter((d) => d.eventId === eventId)
          // 👀 first, before the router or a worker spends any time on it. Intake queued it with the event, except
          // for an observation: atta never reacts in a watched channel.
          if (found.value._tag === "UserMessage" && pending.some((d) => d.target !== "observe")) {
            yield* outbox
              .run(reactionOp(eventId, "seen"))
              .pipe(Effect.catch((error) => Effect.logWarning(`could not react to ${eventId}`, error)))
          }
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
