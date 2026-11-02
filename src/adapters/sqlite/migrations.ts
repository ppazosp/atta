import { SqliteMigrator } from "@effect/sql-sqlite-node"
import { Effect } from "effect"
import { SqlClient } from "effect/sql"

export const migrate = SqliteMigrator.run({
  loader: SqliteMigrator.fromRecord({
    "0001_store": Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient
      yield* sql`
        CREATE TABLE events (
          id TEXT PRIMARY KEY NOT NULL,
          source TEXT NOT NULL,
          external_id TEXT NOT NULL,
          received_at INTEGER NOT NULL,
          data TEXT NOT NULL,
          dispatched_at INTEGER,
          UNIQUE (source, external_id)
        )
      `
      yield* sql`CREATE INDEX events_undispatched ON events (received_at) WHERE dispatched_at IS NULL`
      yield* sql`
        CREATE TABLE threads (
          key TEXT PRIMARY KEY NOT NULL,
          channel TEXT NOT NULL,
          title TEXT,
          last_activity INTEGER NOT NULL
        )
      `
      yield* sql`
        CREATE TABLE workers (
          id TEXT PRIMARY KEY NOT NULL,
          brief TEXT NOT NULL,
          origin TEXT NOT NULL,
          state TEXT NOT NULL,
          runtime_ref TEXT,
          run_id TEXT,
          branch TEXT,
          worktree_path TEXT,
          step_position INTEGER NOT NULL,
          plan_message TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        )
      `
      yield* sql`CREATE INDEX workers_state ON workers (state)`
      yield* sql`
        CREATE TABLE worker_transitions (
          id INTEGER PRIMARY KEY,
          worker_id TEXT NOT NULL REFERENCES workers (id),
          "from" TEXT NOT NULL,
          "to" TEXT NOT NULL,
          input TEXT NOT NULL,
          reason TEXT,
          at INTEGER NOT NULL
        )
      `
      yield* sql`CREATE INDEX worker_transitions_worker ON worker_transitions (worker_id, id)`
      // Thread activity is recorded separately; subscriptions do not require a threads row.
      yield* sql`
        CREATE TABLE subscriptions (
          thread TEXT NOT NULL,
          worker_id TEXT NOT NULL REFERENCES workers (id),
          created_at INTEGER NOT NULL,
          PRIMARY KEY (thread, worker_id)
        )
      `
      yield* sql`CREATE INDEX subscriptions_worker ON subscriptions (worker_id)`
      yield* sql`
        CREATE TABLE deliveries (
          event_id TEXT NOT NULL REFERENCES events (id),
          target TEXT NOT NULL,
          status TEXT NOT NULL,
          attempts INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          PRIMARY KEY (event_id, target)
        )
      `
      yield* sql`CREATE INDEX deliveries_pending ON deliveries (created_at) WHERE status = 'pending'`
      yield* sql`
        CREATE TABLE outbox (
          operation_id TEXT PRIMARY KEY NOT NULL,
          kind TEXT NOT NULL,
          payload TEXT NOT NULL,
          status TEXT NOT NULL,
          result TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        )
      `
      yield* sql`CREATE INDEX outbox_pending ON outbox (created_at) WHERE status = 'pending'`
    }),
  }),
})
