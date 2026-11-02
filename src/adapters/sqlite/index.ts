import { SqliteClient } from "@effect/sql-sqlite-node"
import { Effect, Layer, Option, Schema, String } from "effect"
import { SqlClient } from "effect/sql"
import { Event } from "../../core/domain/event.ts"
import type { EventId, OperationId, Timestamp, WorkerId } from "../../core/domain/ids.ts"
import { ThreadKey } from "../../core/domain/thread.ts"
import type { Worker, WorkerState } from "../../core/domain/worker.ts"
import {
  Store,
  StoreError,
  type DeliveryTarget,
  type OutboxKind,
  type Transition,
} from "../../ports/Store.ts"
import { migrate } from "./migrations.ts"
import { DeliveryRow, EventRow, Json, OutboxRow, SubscriberRow, TransitionRow, WorkerRow } from "./schemas.ts"

const storeError = (operation: string) => (cause: unknown) =>
  new StoreError({ operation, message: globalThis.String(cause) })

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient
  yield* sql`PRAGMA foreign_keys = ON`.pipe(Effect.mapError(storeError("configure")))
  yield* migrate.pipe(Effect.mapError(storeError("migrate")))

  const decodeEvents = Schema.decodeUnknownEffect(Schema.Array(EventRow))
  const encodeEvent = Schema.encodeEffect(Schema.fromJsonString(Event))
  const decodeWorkers = Schema.decodeUnknownEffect(Schema.Array(WorkerRow))
  const encodeWorker = Schema.encodeEffect(WorkerRow)
  const decodeOutbox = Schema.decodeUnknownEffect(Schema.Array(OutboxRow))
  const encodeJson = Schema.encodeUnknownEffect(Json)

  return Store.of({
    transaction: Effect.fn("Store.transaction")(function* <A, E, R>(
      effect: Effect.Effect<A, E, R>,
    ): Effect.fn.Return<A, E | StoreError, R> {
      // Inside a transaction, the SQL client opens a savepoint: a failed inner transaction undoes only its writes.
      return yield* sql
        .withTransaction(effect)
        .pipe(Effect.catchTag("SqlError", (error) => Effect.fail(storeError("transaction")(error))))
    }),
    events: {
      insert: Effect.fn("Store.events.insert")(
        function* (event: Event) {
          const data = yield* encodeEvent(event)
          const inserted = yield* sql`
            INSERT INTO events ${sql.insert({
              id: event.id,
              source: event.source,
              externalId: event.externalId,
              receivedAt: event.receivedAt,
              data,
            })}
            ON CONFLICT (source, external_id) DO NOTHING RETURNING data
          `
          const rows = yield* decodeEvents(
            inserted.length > 0
              ? inserted
              : yield* sql`SELECT data FROM events WHERE source = ${event.source} AND external_id = ${event.externalId}`,
          )
          const stored = rows[0]
          if (stored === undefined) {
            return yield* new StoreError({ operation: "events.insert", message: "Stored event is missing" })
          }
          return { inserted: inserted.length > 0, event: stored.data }
        },
        Effect.mapError(storeError("events.insert")),
      ),
      get: Effect.fn("Store.events.get")(
        function* (id: EventId) {
          const rows = yield* decodeEvents(yield* sql`SELECT data FROM events WHERE id = ${id}`)
          return Option.map(Option.fromNullishOr(rows[0]), (row) => row.data)
        },
        Effect.mapError(storeError("events.get")),
      ),
      markDispatched: Effect.fn("Store.events.markDispatched")(
        (id: EventId, at: Timestamp) => sql`UPDATE events SET dispatched_at = ${at} WHERE id = ${id}`,
        Effect.asVoid,
        Effect.mapError(storeError("events.markDispatched")),
      ),
      undispatched: Effect.fn("Store.events.undispatched")(
        function* () {
          const rows = yield* decodeEvents(
            yield* sql`SELECT data FROM events WHERE dispatched_at IS NULL ORDER BY received_at, rowid`,
          )
          return rows.map((row) => row.data)
        },
        Effect.mapError(storeError("events.undispatched")),
      ),
    },
    threads: {
      touch: Effect.fn("Store.threads.touch")(
        (key: ThreadKey, at: Timestamp) => sql`
          INSERT INTO threads (key, channel, last_activity) VALUES (${key}, 'slack', ${at})
          ON CONFLICT (key) DO UPDATE SET last_activity = excluded.last_activity
        `,
        Effect.asVoid,
        Effect.mapError(storeError("threads.touch")),
      ),
    },
    workers: {
      insert: Effect.fn("Store.workers.insert")(
        function* (worker: Worker) {
          yield* sql`INSERT INTO workers ${sql.insert(yield* encodeWorker(worker))}`
        },
        Effect.mapError(storeError("workers.insert")),
      ),
      get: Effect.fn("Store.workers.get")(
        function* (id: WorkerId) {
          const rows = yield* decodeWorkers(yield* sql`SELECT * FROM workers WHERE id = ${id}`)
          return Option.fromNullishOr(rows[0])
        },
        Effect.mapError(storeError("workers.get")),
      ),
      update: Effect.fn("Store.workers.update")(
        function* (worker: Worker) {
          const { id, ...fields } = yield* encodeWorker(worker)
          yield* sql`UPDATE workers SET ${sql.update(fields)} WHERE id = ${id}`
        },
        Effect.mapError(storeError("workers.update")),
      ),
      inStates: Effect.fn("Store.workers.inStates")(
        function* (states: ReadonlyArray<WorkerState>) {
          if (states.length === 0) return []
          return yield* decodeWorkers(
            yield* sql`SELECT * FROM workers WHERE ${sql.in("state", states)} ORDER BY rowid`,
          )
        },
        Effect.mapError(storeError("workers.inStates")),
      ),
    },
    transitions: {
      append: Effect.fn("Store.transitions.append")(
        function* (transition: Transition) {
          yield* sql`INSERT INTO worker_transitions ${sql.insert(yield* Schema.encodeEffect(TransitionRow)(transition))}`
        },
        Effect.mapError(storeError("transitions.append")),
      ),
      forWorker: Effect.fn("Store.transitions.forWorker")(
        function* (id: WorkerId) {
          return yield* Schema.decodeUnknownEffect(Schema.Array(TransitionRow))(
            yield* sql`SELECT * FROM worker_transitions WHERE worker_id = ${id} ORDER BY id`,
          )
        },
        Effect.mapError(storeError("transitions.forWorker")),
      ),
    },
    subscriptions: {
      subscribe: Effect.fn("Store.subscriptions.subscribe")(
        (thread: ThreadKey, worker: WorkerId, at: Timestamp) => sql`
          INSERT INTO subscriptions (thread, worker_id, created_at) VALUES (${thread}, ${worker}, ${at})
          ON CONFLICT (thread, worker_id) DO NOTHING
        `,
        Effect.asVoid,
        Effect.mapError(storeError("subscriptions.subscribe")),
      ),
      unsubscribe: Effect.fn("Store.subscriptions.unsubscribe")(
        (thread: ThreadKey, worker: WorkerId) =>
          sql`DELETE FROM subscriptions WHERE thread = ${thread} AND worker_id = ${worker}`,
        Effect.asVoid,
        Effect.mapError(storeError("subscriptions.unsubscribe")),
      ),
      subscribers: Effect.fn("Store.subscriptions.subscribers")(
        function* (thread: ThreadKey) {
          return yield* Schema.decodeUnknownEffect(Schema.Array(SubscriberRow))(
            yield* sql`
              SELECT workers.id AS worker_id, workers.state FROM subscriptions
              JOIN workers ON workers.id = subscriptions.worker_id
              WHERE subscriptions.thread = ${thread} ORDER BY subscriptions.rowid
            `,
          )
        },
        Effect.mapError(storeError("subscriptions.subscribers")),
      ),
      threadsOf: Effect.fn("Store.subscriptions.threadsOf")(
        function* (worker: WorkerId) {
          const rows = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ thread: ThreadKey })))(
            yield* sql`SELECT thread FROM subscriptions WHERE worker_id = ${worker} ORDER BY rowid`,
          )
          return rows.map((row) => row.thread)
        },
        Effect.mapError(storeError("subscriptions.threadsOf")),
      ),
    },
    deliveries: {
      record: Effect.fn("Store.deliveries.record")(
        (eventId: EventId, target: DeliveryTarget, at: Timestamp) => sql`
          INSERT INTO deliveries (event_id, target, status, attempts, created_at)
          VALUES (${eventId}, ${target}, 'pending', 0, ${at})
          ON CONFLICT (event_id, target) DO NOTHING
        `,
        Effect.asVoid,
        Effect.mapError(storeError("deliveries.record")),
      ),
      markDone: Effect.fn("Store.deliveries.markDone")(
        (eventId: EventId, target: DeliveryTarget) => sql`
          UPDATE deliveries SET status = 'done', attempts = attempts + 1
          WHERE event_id = ${eventId} AND target = ${target}
        `,
        Effect.asVoid,
        Effect.mapError(storeError("deliveries.markDone")),
      ),
      pending: Effect.fn("Store.deliveries.pending")(
        function* () {
          return yield* Schema.decodeUnknownEffect(Schema.Array(DeliveryRow))(
            yield* sql`SELECT * FROM deliveries WHERE status = 'pending' ORDER BY created_at, rowid`,
          )
        },
        Effect.mapError(storeError("deliveries.pending")),
      ),
    },
    outbox: {
      enqueue: Effect.fn("Store.outbox.enqueue")(
        function* (entry: {
          readonly operationId: OperationId
          readonly kind: OutboxKind
          readonly payload: unknown
          readonly at: Timestamp
        }) {
          const payload = yield* encodeJson(entry.payload)
          yield* sql`
            INSERT INTO outbox (operation_id, kind, payload, status, result, created_at, updated_at)
            VALUES (${entry.operationId}, ${entry.kind}, ${payload}, 'pending', 'null', ${entry.at}, ${entry.at})
            ON CONFLICT (operation_id) DO NOTHING
          `
        },
        Effect.mapError(storeError("outbox.enqueue")),
      ),
      get: Effect.fn("Store.outbox.get")(
        function* (id: OperationId) {
          const rows = yield* decodeOutbox(yield* sql`SELECT * FROM outbox WHERE operation_id = ${id}`)
          return Option.fromNullishOr(rows[0])
        },
        Effect.mapError(storeError("outbox.get")),
      ),
      complete: Effect.fn("Store.outbox.complete")(
        function* (id: OperationId, result: unknown, at: Timestamp) {
          const encoded = yield* encodeJson(result)
          yield* sql`UPDATE outbox SET status = 'done', result = ${encoded}, updated_at = ${at} WHERE operation_id = ${id}`
        },
        Effect.mapError(storeError("outbox.complete")),
      ),
      fail: Effect.fn("Store.outbox.fail")(
        function* (id: OperationId, reason: string, at: Timestamp) {
          const encoded = yield* encodeJson(reason)
          yield* sql`UPDATE outbox SET status = 'failed', result = ${encoded}, updated_at = ${at} WHERE operation_id = ${id}`
        },
        Effect.mapError(storeError("outbox.fail")),
      ),
      pending: Effect.fn("Store.outbox.pending")(
        function* () {
          return yield* decodeOutbox(
            yield* sql`SELECT * FROM outbox WHERE status = 'pending' ORDER BY created_at, rowid`,
          )
        },
        Effect.mapError(storeError("outbox.pending")),
      ),
    },
  })
})

/** Opens and migrates atta's database. The layer's scope owns the connection. */
export const SqliteStoreLayer = (filename: string): Layer.Layer<Store, StoreError> =>
  Layer.effect(Store, make).pipe(
    Layer.provide(
      SqliteClient.layer({
        filename,
        disableWAL: false,
        transformQueryNames: String.camelToSnake,
        transformResultNames: String.snakeToCamel,
      }),
    ),
    Layer.catchTag("SqlError", (error) => Layer.effect(Store, Effect.fail(storeError("open")(error)))),
  )
