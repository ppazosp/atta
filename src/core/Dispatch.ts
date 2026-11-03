import { Context, Effect, Layer, Option, Ref, Schema, Semaphore } from "effect"
import type { Event } from "./domain/event.ts"
import { OperationId, WorkerId, type EventId } from "./domain/ids.ts"
import { isStopCommand } from "./domain/routing.ts"
import { threadKey, type ThreadKey } from "./domain/thread.ts"
import { RouterEngine } from "../ports/RouterEngine.ts"
import { Store, type DeliveryTarget } from "../ports/Store.ts"
import { reactionOp } from "./Intake.ts"
import { Outbox, Results } from "./Outbox.ts"
import { now } from "./clock.ts"
import { renderChannelBatch, renderEvent } from "./render.ts"
import { Settings } from "./Settings.ts"
import { USER_MESSAGE, Workers } from "./Workers.ts"

/** The outbox operation that sends the approval request for a proposal. */
export const approvalOp = (proposalId: EventId) => OperationId.make(`approval:${proposalId}`)

/** The most watched-channel messages one channel turn reads. The rest wait for the next turn. */
const MAX_BATCH = 50

/**
 * Delivers routed events: to the router as an input, or straight to a worker. When the event is a Slack message,
 * 👀 becomes ✅ once it is handled: when the router has answered it, or when the worker has taken it.
 *
 * Watched-channel messages go to the router in batches, one channel turn at a time: while the router works on one,
 * new messages wait, then all go in the next. Each batch is its own input, never mixed with the user's.
 */

/** A proposal's worker ID is fixed by the proposal, so a replayed Start finds the worker it already started. */
const proposalSeed = (proposal: Extract<Event, { _tag: "WorkProposed" }>) =>
  proposal.id.replace(/[^A-Za-z0-9_-]/g, "-")
const proposalWorker = (proposal: Extract<Event, { _tag: "WorkProposed" }>) =>
  WorkerId.make(`w-${proposalSeed(proposal)}`)

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
      const settings = yield* Settings
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
        const proposal = event._tag === "ApprovalDecision" ? yield* findProposal(event.proposalId) : null
        const workerId =
          event._tag === "WorkerReport" || event._tag === "WorkerFinished"
            ? event.workerId
            : proposal !== null
              ? proposalWorker(proposal)
              : null
        const worker = workerId === null ? null : Option.getOrNull(yield* store.workers.get(workerId))
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
          content: renderEvent(event, worker, opened, proposal),
          whenBusy: "queue",
        })
        yield* engine.settled(event.id).pipe(
          Effect.flatMap((settled) => markHandled(event, settled.status === "answered" ? "done" : "failed")),
          Effect.catch((error) => Effect.logWarning(`could not finish event ${event.id}`, error)),
          Effect.forkIn(scope),
        )
      })

      // The request ID of the channel turn the router is on, if any. Lost on restart: then a second batch may queue
      // behind one Pi resumes, which only means a smaller turn.
      const channelTurn = yield* Ref.make<string | null>(null)
      const flushing = yield* Semaphore.make(1)

      /** Sends every waiting watched-channel message to the router as one input, unless a channel turn is on. */
      const flushChannel: Effect.Effect<void> = flushing
        .withPermits(1)(
          Effect.gen(function* () {
            if ((yield* Ref.get(channelTurn)) !== null) return
            const waiting = (yield* store.deliveries.pending())
              .filter((d) => d.target === "observe")
              .toSorted((a, b) => a.createdAt - b.createdAt)
              .slice(0, MAX_BATCH)
            if (waiting.length === 0) return
            const messages: Array<Extract<Event, { _tag: "UserMessage" }>> = []
            for (const delivery of waiting) {
              const found = yield* store.events.get(delivery.eventId)
              if (Option.isSome(found) && found.value._tag === "UserMessage") messages.push(found.value)
            }
            // Named after its newest message: a retry after a crash, with no new messages, is the same input.
            const requestId = `channel:${waiting.at(-1)!.eventId}`
            if (messages.length > 0) {
              yield* engine.submit({
                requestId,
                content: renderChannelBatch(messages, settings.slack.trustedUserId),
                whenBusy: "queue",
              })
            }
            // All at once: a crash between the submit and this commit resends the whole batch, which at worst
            // shows the router messages again, never a part of a batch twice beside new ones.
            yield* store.transaction(
              Effect.forEach(waiting, (d) => store.deliveries.markDone(d.eventId, "observe"), {
                discard: true,
              }),
            )
            if (messages.length === 0) return
            yield* Ref.set(channelTurn, requestId)
            const started = yield* now
            yield* engine.settled(requestId).pipe(
              Effect.tap((settled) =>
                Effect.gen(function* () {
                  const seconds = Math.round(((yield* now) - started) / 1000)
                  yield* Effect.logInfo(
                    `channel turn ${requestId}: ${messages.length} messages, ${settled.status}, ${seconds}s`,
                  )
                }),
              ),
              Effect.catch((error) => Effect.logWarning(`channel turn ${requestId} failed`, error)),
              Effect.andThen(Ref.set(channelTurn, null)),
              Effect.andThen(Effect.suspend(() => flushChannel)),
              Effect.forkIn(scope),
            )
          }),
        )
        .pipe(Effect.catch((error) => Effect.logWarning("sending channel messages failed", error)))

      /**
       * The user's Start or Skip on a proposal. Start opens the channel thread and starts the worker there; either
       * way the approval message loses its buttons. The worker ID comes from the proposal, so a replay starts no
       * second worker.
       */
      const decide = Effect.fn("Dispatch.decide")(function* (event: Event) {
        if (event._tag !== "ApprovalDecision") return
        const proposal = yield* findProposal(event.proposalId)
        if (proposal === null) return yield* Effect.logWarning(`decision ${event.id} names no proposal`)
        let text = `Skipped: ${proposal.title}`
        if (event.decision === "approve") {
          const origin = threadKey(proposal.thread)
          yield* store.threads.touch(origin, yield* now)
          const seed = proposalSeed(proposal)
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
          const worker = yield* store.workers.get(proposalWorker(proposal))
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

      const findProposal = Effect.fn("Dispatch.findProposal")(function* (id: EventId) {
        const found = yield* store.events.get(id)
        return Option.isSome(found) && found.value._tag === "WorkProposed" ? found.value : null
      })

      const toWorker = Effect.fn("Dispatch.toWorker")(function* (event: Event, workerId: WorkerId) {
        if (event._tag !== "UserMessage") return yield* toRouter(event)
        const handled = isStopCommand(event.text)
          ? workers.stop({ workerId, operationId: OperationId.make(`stop:${event.id}:${workerId}`) })
          : workers.message({
              workerId,
              text: `${USER_MESSAGE}\n\n${event.text}`,
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
        else if (target === "core") yield* decide(event)
        else yield* toWorker(event, WorkerId.make(target.slice("worker:".length)))
        yield* store.deliveries.markDone(event.id, target)
      })

      const deliver = Effect.fn("Dispatch.deliver")(
        function* (eventId: EventId) {
          const found = yield* store.events.get(eventId)
          if (Option.isNone(found)) return
          // Core acts on a decision before the router hears what came of it.
          const pending = (yield* store.deliveries.pending())
            .filter((d) => d.eventId === eventId)
            .toSorted((a, b) => (a.target === "core" ? 0 : 1) - (b.target === "core" ? 0 : 1))
          // 👀 first, before the router or a worker spends any time on it. Intake queued it with the event, except
          // for an observation: atta never reacts in a watched channel.
          if (found.value._tag === "UserMessage" && pending.some((d) => d.target !== "observe")) {
            yield* outbox
              .run(reactionOp(eventId, "seen"))
              .pipe(Effect.catch((error) => Effect.logWarning(`could not react to ${eventId}`, error)))
          }
          for (const delivery of pending) {
            if (delivery.target === "observe") {
              yield* flushChannel
              continue
            }
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
