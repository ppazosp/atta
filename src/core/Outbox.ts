import { Context, Effect, Layer, Option, Ref, Result, Schema, Semaphore } from "effect"
import { RunId, RuntimeRef, WorkerId, type OperationId } from "./domain/ids.ts"
import { Conversation, MessageRef, ThreadRef } from "./domain/thread.ts"
import { Brief, RunOutcome } from "./domain/worker.ts"
import { Channel, type ChannelError } from "../ports/Channel.ts"
import { Runtime, type RuntimeError } from "../ports/Runtime.ts"
import { Store, StoreError, type OutboxEntry, type OutboxKind } from "../ports/Store.ts"
import { now } from "./clock.ts"

const ReplyTarget = Schema.Union([
  Schema.Struct({ thread: ThreadRef }),
  Schema.Struct({ conversation: Conversation }),
])
const AskMessage = Schema.Struct({ message: MessageRef, files: Schema.Array(Schema.String) })
const PlanTask = Schema.Struct({
  id: Schema.String,
  title: Schema.String,
  status: Schema.Literals(["pending", "in_progress", "complete", "error"]),
  detail: Schema.NullOr(Schema.String),
})

/** What each outbox kind carries. Stored as JSON, decoded when the action runs. */
const Payloads = {
  slack_reply: Schema.Struct({ target: ReplyTarget, text: Schema.String }),
  slack_react: Schema.Struct({ message: MessageRef, emoji: Schema.String }),
  slack_unreact: Schema.Struct({ message: MessageRef, emoji: Schema.String }),
  plan_upsert: Schema.Struct({ thread: ThreadRef, title: Schema.String }),
  plan_finish: Schema.Struct({
    plan: MessageRef,
    title: Schema.String,
    tasks: Schema.Array(PlanTask),
    outcome: RunOutcome,
    answer: Schema.NullOr(Schema.String),
  }),
  worker_launch: Schema.Struct({ workerId: WorkerId, repo: Schema.String, brief: Brief }),
  worker_send: Schema.Struct({
    workerId: WorkerId,
    worker: RuntimeRef,
    text: Schema.String,
    mode: Schema.Literals(["steer", "queue"]),
  }),
  worker_stop: Schema.Struct({ workerId: WorkerId, worker: RuntimeRef }),
  approval_request: Schema.Struct({
    ownerUserId: Schema.String,
    proposalId: Schema.String,
    thread: ThreadRef,
    repo: Schema.String,
    title: Schema.String,
    why: Schema.String,
    brief: Schema.String,
  }),
  approval_close: Schema.Struct({ message: MessageRef, text: Schema.String }),
  ask_post: Schema.Struct({
    thread: ThreadRef,
    askId: Schema.String,
    text: Schema.String,
    images: Schema.Array(Schema.String),
    worktree: Schema.NullOr(Schema.String),
  }),
  ask_close: Schema.Struct({ ask: AskMessage, text: Schema.String, outcome: Schema.String }),
} satisfies Record<OutboxKind, Schema.Top>

export type Payload<K extends OutboxKind> = (typeof Payloads)[K]["Type"]

export const decodePayload = <K extends OutboxKind>(kind: K, value: unknown) =>
  decode(Payloads[kind], value, `outbox.${kind}`) as Effect.Effect<Payload<K>, StoreError>

/** Results kept for kinds whose result the caller needs again after a crash. */
export const Results = {
  slack_reply: MessageRef,
  plan_upsert: MessageRef,
  worker_launch: Schema.Struct({
    runtimeRef: RuntimeRef,
    runId: RunId,
    branch: Schema.NullOr(Schema.String),
    worktreePath: Schema.NullOr(Schema.String),
  }),
  worker_send: Schema.Struct({ runId: RunId }),
  approval_request: MessageRef,
  ask_post: AskMessage,
}

const decode = <S extends Schema.Top>(schema: S, value: unknown, operation: string) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError((e) => new StoreError({ operation, message: `bad outbox payload: ${e.message}` })),
  ) as Effect.Effect<S["Type"], StoreError>

export type OutboxFailure = ChannelError | RuntimeError | StoreError

/**
 * What to do with an action's outcome, in the transaction that records it:
 * - `apply` records the result where it belongs (a worker's new run), together with marking the entry done
 * - `onFailure`, when given, makes a failure final: the entry is marked failed and the failure recorded with it.
 *   Without it, a failure leaves the entry pending, to be tried again
 */
export interface Handlers<E> {
  readonly apply?: (result: unknown) => Effect.Effect<void, E>
  readonly onFailure?: (error: ChannelError | RuntimeError | StoreError) => Effect.Effect<void, E>
}

/**
 * Runs actions outside atta through the outbox: write the intent in a transaction (`enqueue`), commit, then
 * `run` it. `run` is idempotent: a done entry returns its stored result without acting again.
 */
export class Outbox extends Context.Service<
  Outbox,
  {
    /** Call inside the caller's transaction. Enqueuing an existing operation ID does nothing. */
    enqueue<K extends OutboxKind>(
      operationId: OperationId,
      kind: K,
      payload: Payload<K>,
    ): Effect.Effect<void, StoreError>
    /** Runs a pending entry, or returns the result of a done one (whose handlers already ran). */
    run<E = never>(
      operationId: OperationId,
      handlers?: Handlers<E>,
    ): Effect.Effect<unknown, OutboxFailure | E>
    /** Runs every pending entry, with the handlers `handlersFor` gives each. Used once at startup. */
    recover<E>(handlersFor: (entry: OutboxEntry) => Handlers<E>): Effect.Effect<void>
  }
>()("atta/core/Outbox") {
  static readonly layer = Layer.effect(
    Outbox,
    Effect.gen(function* () {
      const store = yield* Store
      const channel = yield* Channel
      const runtime = yield* Runtime
      // Entries written before this process started may have run already; messages are looked up first.
      const startedAt = yield* now

      const act = Effect.fn("Outbox.act")(function* (entry: OutboxEntry) {
        const id = entry.operationId
        switch (entry.kind) {
          case "slack_reply": {
            const p = yield* decode(Payloads.slack_reply, entry.payload, "outbox.slack_reply")
            if (entry.createdAt <= startedAt) {
              const found = yield* channel.findReply({ target: p.target, operationId: id })
              if (Option.isSome(found)) return found.value
            }
            return yield* channel.reply({ target: p.target, text: p.text, operationId: id })
          }
          case "slack_react": {
            const p = yield* decode(Payloads.slack_react, entry.payload, "outbox.slack_react")
            return yield* channel.react(p.message, p.emoji).pipe(Effect.as(null))
          }
          case "slack_unreact": {
            const p = yield* decode(Payloads.slack_unreact, entry.payload, "outbox.slack_unreact")
            return yield* channel.unreact(p.message, p.emoji).pipe(Effect.as(null))
          }
          case "plan_upsert": {
            const p = yield* decode(Payloads.plan_upsert, entry.payload, "outbox.plan_upsert")
            // The plan message carries its operation ID too, so a crash after posting does not post it twice.
            if (entry.createdAt <= startedAt) {
              const found = yield* channel.findReply({ target: { thread: p.thread }, operationId: id })
              if (Option.isSome(found)) return found.value
            }
            return yield* channel.upsertPlan({
              thread: p.thread,
              plan: null,
              title: p.title,
              tasks: [],
              operationId: id,
            })
          }
          case "plan_finish": {
            const p = yield* decode(Payloads.plan_finish, entry.payload, "outbox.plan_finish")
            return yield* channel.finishPlan(p).pipe(Effect.as(null))
          }
          case "worker_launch": {
            const p = yield* decode(Payloads.worker_launch, entry.payload, "outbox.worker_launch")
            return yield* runtime.launch({ repo: p.repo, brief: p.brief, operationId: id })
          }
          case "worker_send": {
            const p = yield* decode(Payloads.worker_send, entry.payload, "outbox.worker_send")
            return yield* runtime.send({ worker: p.worker, text: p.text, mode: p.mode, operationId: id })
          }
          case "approval_request": {
            const p = yield* decode(Payloads.approval_request, entry.payload, "outbox.approval_request")
            return yield* channel.requestApproval({ ...p, operationId: id })
          }
          case "approval_close": {
            const p = yield* decode(Payloads.approval_close, entry.payload, "outbox.approval_close")
            return yield* channel.closeApproval(p).pipe(Effect.as(null))
          }
          case "ask_post": {
            const p = yield* decode(Payloads.ask_post, entry.payload, "outbox.ask_post")
            // The adapter finds a request it already posted by the operation ID, so a retry does not post twice.
            return yield* channel.askApproval({ ...p, operationId: id })
          }
          case "ask_close": {
            const p = yield* decode(Payloads.ask_close, entry.payload, "outbox.ask_close")
            return yield* channel.closeAsk(p).pipe(Effect.as(null))
          }
          case "worker_stop": {
            const p = yield* decode(Payloads.worker_stop, entry.payload, "outbox.worker_stop")
            return yield* runtime.stop({ worker: p.worker, operationId: id }).pipe(Effect.as(null))
          }
        }
      })

      // One run of an operation at a time: a second caller waits, then finds it done instead of acting again.
      const running = yield* Ref.make<ReadonlyMap<string, Semaphore.Semaphore>>(new Map())
      const lockFor = (operationId: OperationId) =>
        Ref.modify(
          running,
          (all): readonly [Semaphore.Semaphore, ReadonlyMap<string, Semaphore.Semaphore>] => {
            const existing = all.get(operationId)
            if (existing) return [existing, all]
            const created = Semaphore.makeUnsafe(1)
            return [created, new Map(all).set(operationId, created)]
          },
        )

      const runUnlocked = Effect.fn("Outbox.run")(function* <E>(
        operationId: OperationId,
        handlers: Handlers<E> = {},
      ) {
        const found = yield* store.outbox.get(operationId)
        if (Option.isNone(found)) {
          return yield* new StoreError({ operation: "outbox.run", message: `no outbox entry ${operationId}` })
        }
        const entry = found.value
        if (entry.status === "done") return entry.result
        if (entry.status === "failed") {
          return yield* new StoreError({
            operation: "outbox.run",
            message: `${operationId} failed: ${String(entry.result)}`,
          })
        }
        const outcome = yield* act(entry).pipe(Effect.result)
        if (Result.isFailure(outcome)) {
          const onFailure = handlers.onFailure
          if (onFailure !== undefined) {
            yield* store.transaction(
              Effect.gen(function* () {
                yield* store.outbox.fail(operationId, outcome.failure.message, yield* now)
                yield* onFailure(outcome.failure)
              }),
            )
          }
          return yield* Effect.fail(outcome.failure)
        }
        const apply = handlers.apply
        yield* store.transaction(
          Effect.gen(function* () {
            yield* store.outbox.complete(operationId, outcome.success, yield* now)
            if (apply !== undefined) yield* apply(outcome.success)
          }),
        )
        return outcome.success
      })

      const run = <E = never>(operationId: OperationId, handlers: Handlers<E> = {}) =>
        lockFor(operationId).pipe(
          Effect.flatMap((lock) => lock.withPermits(1)(runUnlocked(operationId, handlers))),
        )

      return Outbox.of({
        enqueue: (operationId, kind, payload) =>
          now.pipe(Effect.flatMap((at) => store.outbox.enqueue({ operationId, kind, payload, at }))),
        run,
        recover: (handlersFor) =>
          store.outbox.pending().pipe(
            Effect.flatMap((entries) =>
              Effect.forEach(entries, (entry) =>
                run(entry.operationId, handlersFor(entry)).pipe(
                  Effect.catch((error) =>
                    Effect.logWarning(`outbox ${entry.operationId} failed on recovery`, error),
                  ),
                ),
              ),
            ),
            Effect.catch((error) => Effect.logError("outbox recovery could not read pending entries", error)),
            Effect.asVoid,
          ),
      })
    }),
  )
}
