import type { Effect, Option } from "effect"
import { Context, Schema } from "effect"
import type { Event } from "../core/domain/event.ts"
import type { EventId, OperationId, Timestamp, WorkerId } from "../core/domain/ids.ts"
import type { WorkerInput } from "../core/domain/lifecycle.ts"
import type { Subscriber } from "../core/domain/routing.ts"
import type { ThreadKey } from "../core/domain/thread.ts"
import type { Worker, WorkerState } from "../core/domain/worker.ts"

export class StoreError extends Schema.TaggedError<StoreError>()("StoreError", {
  operation: Schema.String,
  message: Schema.String,
}) {}

export interface Transition {
  readonly workerId: WorkerId
  readonly from: WorkerState
  readonly to: WorkerState
  readonly input: WorkerInput
  readonly reason: string | null
  readonly at: Timestamp
}

/** Where an event was sent. `worker:<id>` targets a worker directly (pass-through). */
export type DeliveryTarget = "router" | `worker:${string}`

export interface Delivery {
  readonly eventId: EventId
  readonly target: DeliveryTarget
  readonly status: "pending" | "done"
  readonly attempts: number
  readonly createdAt: Timestamp
}

export type OutboxKind =
  | "slack_reply"
  | "slack_react"
  | "slack_unreact"
  | "plan_upsert"
  | "plan_finish"
  | "worker_launch"
  | "worker_send"
  | "worker_stop"

/** One action atta takes outside. Written before the action, marked done with its result after. */
export interface OutboxEntry {
  readonly operationId: OperationId
  readonly kind: OutboxKind
  /** The action's input, as JSON. Each kind's handler in core decodes it. */
  readonly payload: unknown
  readonly status: "pending" | "done" | "failed"
  /** The action's result, as JSON (for example the posted Slack message). */
  readonly result: unknown
  readonly createdAt: Timestamp
  readonly updatedAt: Timestamp
}

/** atta's own SQLite database, the source of truth. */
export class Store extends Context.Service<
  Store,
  {
    /** Runs `effect` in one transaction: every write inside lands, or none does. */
    transaction<A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E | StoreError, R>

    readonly events: {
      /** Stores the event unless one with the same source and external ID exists. Returns the stored one. */
      insert(event: Event): Effect.Effect<{ readonly inserted: boolean; readonly event: Event }, StoreError>
      get(id: EventId): Effect.Effect<Option.Option<Event>, StoreError>
    }

    readonly threads: {
      /** Records activity in a thread, creating it the first time. */
      touch(key: ThreadKey, at: Timestamp): Effect.Effect<void, StoreError>
    }

    readonly workers: {
      insert(worker: Worker): Effect.Effect<void, StoreError>
      get(id: WorkerId): Effect.Effect<Option.Option<Worker>, StoreError>
      update(worker: Worker): Effect.Effect<void, StoreError>
      inStates(states: ReadonlyArray<WorkerState>): Effect.Effect<ReadonlyArray<Worker>, StoreError>
    }

    readonly transitions: {
      append(transition: Transition): Effect.Effect<void, StoreError>
      forWorker(id: WorkerId): Effect.Effect<ReadonlyArray<Transition>, StoreError>
    }

    readonly subscriptions: {
      subscribe(thread: ThreadKey, worker: WorkerId, at: Timestamp): Effect.Effect<void, StoreError>
      unsubscribe(thread: ThreadKey, worker: WorkerId): Effect.Effect<void, StoreError>
      /** Workers subscribed to the thread, with their current state. */
      subscribers(thread: ThreadKey): Effect.Effect<ReadonlyArray<Subscriber>, StoreError>
      threadsOf(worker: WorkerId): Effect.Effect<ReadonlyArray<ThreadKey>, StoreError>
    }

    readonly deliveries: {
      /** Records that the event goes to `target`. Recording the same pair twice keeps the first. */
      record(eventId: EventId, target: DeliveryTarget, at: Timestamp): Effect.Effect<void, StoreError>
      markDone(eventId: EventId, target: DeliveryTarget): Effect.Effect<void, StoreError>
      pending(): Effect.Effect<ReadonlyArray<Delivery>, StoreError>
    }

    readonly outbox: {
      /** Adds an entry unless one with the same operation ID exists. */
      enqueue(entry: {
        readonly operationId: OperationId
        readonly kind: OutboxKind
        readonly payload: unknown
        readonly at: Timestamp
      }): Effect.Effect<void, StoreError>
      get(id: OperationId): Effect.Effect<Option.Option<OutboxEntry>, StoreError>
      complete(id: OperationId, result: unknown, at: Timestamp): Effect.Effect<void, StoreError>
      fail(id: OperationId, reason: string, at: Timestamp): Effect.Effect<void, StoreError>
      pending(): Effect.Effect<ReadonlyArray<OutboxEntry>, StoreError>
    }
  }
>()("atta/ports/Store") {}
