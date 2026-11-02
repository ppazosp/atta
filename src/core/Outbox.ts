import { Context, Effect, Layer, Option, Schema } from "effect"
import { RunId, RuntimeRef, type OperationId } from "./domain/ids.ts"
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
  worker_launch: Schema.Struct({ repo: Schema.String, brief: Brief }),
  worker_send: Schema.Struct({
    worker: RuntimeRef,
    text: Schema.String,
    mode: Schema.Literals(["steer", "queue"]),
  }),
  worker_stop: Schema.Struct({ worker: RuntimeRef }),
} satisfies Record<OutboxKind, Schema.Top>

export type Payload<K extends OutboxKind> = (typeof Payloads)[K]["Type"]

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
}

const decode = <S extends Schema.Top>(schema: S, value: unknown, operation: string) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError((e) => new StoreError({ operation, message: `bad outbox payload: ${e.message}` })),
  ) as Effect.Effect<S["Type"], StoreError>

export type OutboxFailure = ChannelError | RuntimeError | StoreError

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
    /** Runs a pending entry, or returns the result of a done one. */
    run(operationId: OperationId): Effect.Effect<unknown, OutboxFailure>
    /** Runs every pending entry. Used once at startup. */
    readonly recover: Effect.Effect<void>
  }
>()("atta/core/Outbox") {
  static readonly layer = Layer.effect(
    Outbox,
    Effect.gen(function* () {
      const store = yield* Store
      const channel = yield* Channel
      const runtime = yield* Runtime
      // Entries written before this process started may have run already; replies are looked up first.
      const startedAt = yield* now

      const act = Effect.fn("Outbox.act")(function* (entry: OutboxEntry) {
        const id = entry.operationId
        switch (entry.kind) {
          case "slack_reply": {
            const p = yield* decode(Payloads.slack_reply, entry.payload, "outbox.slack_reply")
            if (entry.createdAt < startedAt) {
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
          case "worker_stop": {
            const p = yield* decode(Payloads.worker_stop, entry.payload, "outbox.worker_stop")
            return yield* runtime.stop({ worker: p.worker, operationId: id }).pipe(Effect.as(null))
          }
        }
      })

      const run = Effect.fn("Outbox.run")(function* (operationId: OperationId) {
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
        // A failure leaves the entry pending, so the next run or the next startup tries again.
        const result = yield* act(entry)
        yield* store.outbox.complete(operationId, result, yield* now)
        return result
      })

      return Outbox.of({
        enqueue: (operationId, kind, payload) =>
          now.pipe(Effect.flatMap((at) => store.outbox.enqueue({ operationId, kind, payload, at }))),
        run,
        recover: store.outbox.pending().pipe(
          Effect.flatMap((entries) =>
            Effect.forEach(entries, (entry) =>
              run(entry.operationId).pipe(
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
