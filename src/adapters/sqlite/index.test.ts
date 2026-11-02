import { describe, expect, it } from "@effect/vitest"
import { type Cause, Deferred, Effect, Fiber, Option, Schema } from "effect"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { DatabaseSync } from "node:sqlite"
import { Event } from "../../core/domain/event.ts"
import { EventId, OperationId, RunId, RuntimeRef, Timestamp, WorkerId } from "../../core/domain/ids.ts"
import { allArrows } from "../../core/domain/lifecycle.ts"
import { ThreadKey } from "../../core/domain/thread.ts"
import { Worker, WorkerState } from "../../core/domain/worker.ts"
import { Store, StoreError, type Transition } from "../../ports/Store.ts"
import { SqliteStoreLayer } from "./index.ts"

const at = Timestamp.make(100)
const later = Timestamp.make(200)
const thread = ThreadKey.make("slack:T-TEST/C-TEST/100.001")
const otherThread = ThreadKey.make("slack:T-TEST/C-TEST/200.001")
const op = OperationId.make

const event: Event = {
  _tag: "UserMessage",
  id: EventId.make("event-1"),
  source: "slack",
  externalId: "external-1",
  receivedAt: at,
  thread: { team: "T-TEST", conversation: "C-TEST", rootTs: "100.001" },
  message: { team: "T-TEST", conversation: "C-TEST", ts: "100.002" },
  userId: "U-TEST",
  text: "Quotes ' and unicode 🐜\nline two",
  via: "mention",
}

const worker: Worker = {
  id: WorkerId.make("worker-1"),
  brief: { repo: "sandbox-repo", title: "Fix a bug", text: "Keep 'quotes' and 🐜\nnewlines" },
  origin: thread,
  state: "launching",
  runtimeRef: null,
  runId: null,
  branch: null,
  worktreePath: null,
  stepPosition: 0,
  planMessage: null,
  createdAt: at,
  updatedAt: at,
}

const withDatabase = Effect.fn("SqliteTest.withDatabase")(function* <A, E, R>(
  run: (filename: string) => Effect.Effect<A, E, R>,
): Effect.fn.Return<A, E | Cause.UnknownError, R> {
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.tryPromise(() => mkdtemp(join(tmpdir(), "atta-sqlite-"))),
        (path) => Effect.promise(() => rm(path, { recursive: true, force: true })),
      )
      return yield* run(join(directory, "store.db"))
    }),
  )
})

const inspect = Effect.fn("SqliteTest.inspect")(function* <A>(
  filename: string,
  run: (db: DatabaseSync) => A,
) {
  return yield* Effect.acquireUseRelease(
    Effect.try(() => new DatabaseSync(filename)),
    (db) => Effect.try(() => run(db)),
    (db) => Effect.sync(() => db.close()),
  )
})

describe("SQLite store", () => {
  it.effect("migrates once, enables WAL and foreign keys, and reopens persisted data", () =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        yield* Effect.gen(function* () {
          const store = yield* Store
          yield* store.events.insert(event)
          yield* store.workers.insert(worker)
          const error = yield* store.subscriptions
            .subscribe(thread, WorkerId.make("missing"), at)
            .pipe(Effect.flip)
          expect(error).toBeInstanceOf(StoreError)
          expect(error.operation).toBe("subscriptions.subscribe")
        }).pipe(Effect.provide(SqliteStoreLayer(filename)))

        yield* Effect.gen(function* () {
          const store = yield* Store
          expect(yield* store.events.get(event.id)).toEqual(Option.some(event))
          expect(yield* store.workers.get(worker.id)).toEqual(Option.some(worker))
          yield* inspect(filename, (db) => {
            expect(db.prepare("PRAGMA journal_mode").get()).toMatchObject({ journal_mode: "wal" })
            expect(db.prepare("SELECT COUNT(*) AS count FROM effect_sql_migrations").get()).toMatchObject({
              count: 1,
            })
            expect(
              db
                .prepare(
                  "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'effect_%' ORDER BY name",
                )
                .all()
                .map((row) => row.name),
            ).toEqual([
              "deliveries",
              "events",
              "outbox",
              "subscriptions",
              "threads",
              "worker_transitions",
              "workers",
            ])
          })
        }).pipe(Effect.provide(SqliteStoreLayer(filename)))
      }),
    ),
  )

  it.effect("keeps the original event on duplicate source and external ID, including after dispatch", () =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        const store = yield* Store
        expect(yield* store.events.get(event.id)).toEqual(Option.none())
        expect(yield* store.events.insert(event)).toEqual({ inserted: true, event })
        yield* store.events.markDispatched(event.id, later)
        expect(
          yield* store.events.insert({
            ...event,
            id: EventId.make("retry"),
            text: "changed",
            receivedAt: later,
          }),
        ).toEqual({ inserted: false, event })
        expect(yield* store.events.get(EventId.make("retry"))).toEqual(Option.none())
        expect(yield* store.events.undispatched()).toEqual([])
        const anotherSource: Event = {
          _tag: "WorkerReport",
          id: EventId.make("worker-event"),
          source: "worker",
          externalId: event.externalId,
          receivedAt: at,
          workerId: worker.id,
          text: "done",
        }
        expect(yield* store.events.insert(anotherSource)).toEqual({ inserted: true, event: anotherSource })
        const collision = yield* store.events
          .insert({ ...event, externalId: "another-external" })
          .pipe(Effect.flip)
        expect(collision.operation).toBe("events.insert")
      }).pipe(Effect.provide(SqliteStoreLayer(filename))),
    ),
  )

  it.effect(
    "recovers every event kind in timestamp order, with stable ties, excluding dispatched events",
    () =>
      withDatabase((filename) =>
        Effect.gen(function* () {
          const store = yield* Store
          const events: ReadonlyArray<Event> = [
            { ...event, receivedAt: later },
            {
              _tag: "ScheduledPrompt",
              id: EventId.make("schedule"),
              source: "clock",
              externalId: "slot",
              receivedAt: at,
              schedule: "daily",
              slot: at,
              target: { conversation: { team: "T-TEST", conversation: "C-TEST" } },
              text: "summarise",
            },
            {
              _tag: "WorkerReport",
              id: EventId.make("report"),
              source: "worker",
              externalId: "report",
              receivedAt: at,
              workerId: worker.id,
              text: "done",
            },
            {
              _tag: "WorkerFinished",
              id: EventId.make("finished"),
              source: "worker",
              externalId: "finished",
              receivedAt: Timestamp.make(0),
              workerId: worker.id,
              runId: RunId.make("run"),
              outcome: "completed",
              finalMessage: "done",
              branch: "atta/test",
              commit: "abc123",
              wrongBranch: false,
            },
          ]
          for (const item of events) {
            yield* store.events.insert(item)
            expect(yield* store.events.get(item.id)).toEqual(Option.some(item))
          }
          expect((yield* store.events.undispatched()).map((item) => item.id)).toEqual([
            "finished",
            "schedule",
            "report",
            event.id,
          ])
          yield* store.events.markDispatched(EventId.make("finished"), at)
          yield* store.events.markDispatched(event.id, later)
          expect((yield* store.events.undispatched()).map((item) => item.id)).toEqual(["schedule", "report"])
          yield* inspect(filename, (db) =>
            expect(db.prepare("SELECT dispatched_at FROM events WHERE id = ?").get(event.id)).toMatchObject({
              dispatched_at: later,
            }),
          )
        }).pipe(Effect.provide(SqliteStoreLayer(filename))),
      ),
  )

  it.effect("touches a thread without replacing its title", () =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        const store = yield* Store
        yield* store.threads.touch(thread, at)
        yield* inspect(filename, (db) =>
          db.prepare("UPDATE threads SET title = 'A conversation' WHERE key = ?").run(thread),
        )
        yield* store.threads.touch(thread, later)
        yield* inspect(filename, (db) =>
          expect(db.prepare("SELECT * FROM threads").all()).toEqual([
            { key: thread, channel: "slack", title: "A conversation", last_activity: later },
          ]),
        )
      }).pipe(Effect.provide(SqliteStoreLayer(filename))),
    ),
  )

  it.effect("round-trips every worker field, nulls, updates and state filters", () =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        const store = yield* Store
        expect(yield* store.workers.get(worker.id)).toEqual(Option.none())
        yield* store.workers.insert(worker)
        expect(yield* store.workers.get(worker.id)).toEqual(Option.some(worker))
        const updated: Worker = {
          ...worker,
          brief: { repo: "other-repo", title: "Updated", text: "New instructions" },
          origin: otherThread,
          state: "running",
          runtimeRef: RuntimeRef.make("t3-thread"),
          runId: RunId.make("run-1"),
          branch: "atta/test",
          worktreePath: "/tmp/test worktree",
          stepPosition: 42,
          planMessage: { team: "T-TEST", conversation: "C-TEST", ts: "100.003" },
          updatedAt: later,
        }
        yield* store.workers.update(updated)
        expect(yield* store.workers.get(worker.id)).toEqual(Option.some(updated))
        yield* store.workers.insert({ ...worker, id: WorkerId.make("idle-worker"), state: "idle" })
        expect(yield* store.workers.inStates([])).toEqual([])
        expect(yield* store.workers.inStates(["launching", "failed"])).toEqual([])
        expect(yield* store.workers.inStates(["running"])).toEqual([updated])
        expect((yield* store.workers.inStates(["running", "idle"])).map((item) => item.state)).toEqual([
          "running",
          "idle",
        ])
        for (const state of WorkerState.literals) {
          const next = { ...worker, state, updatedAt: later }
          yield* store.workers.update(next)
          expect(yield* store.workers.get(worker.id)).toEqual(Option.some(next))
        }
        expect((yield* store.workers.insert(worker).pipe(Effect.flip)).operation).toBe("workers.insert")
      }).pipe(Effect.provide(SqliteStoreLayer(filename))),
    ),
  )

  it.effect("appends every transition with its reason and preserves append order", () =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        const store = yield* Store
        yield* store.workers.insert(worker)
        expect(yield* store.transitions.forWorker(worker.id)).toEqual([])
        const transitions: ReadonlyArray<Transition> = allArrows().map(([from, input, to], index) => ({
          workerId: worker.id,
          from,
          input,
          to,
          reason: index % 2 === 0 ? null : "because 'reason' 🐜",
          at: Timestamp.make(100 - index),
        }))
        for (const transition of transitions) yield* store.transitions.append(transition)
        expect(yield* store.transitions.forWorker(worker.id)).toEqual(transitions)
        expect(yield* store.transitions.forWorker(WorkerId.make("other"))).toEqual([])
      }).pipe(Effect.provide(SqliteStoreLayer(filename))),
    ),
  )

  it.effect("keeps subscriptions unique and joins each worker's current state", () =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        const store = yield* Store
        const second = { ...worker, id: WorkerId.make("worker-2"), state: "idle" as const }
        yield* store.workers.insert(worker)
        yield* store.workers.insert(second)
        yield* store.subscriptions.subscribe(thread, worker.id, at)
        yield* store.subscriptions.subscribe(thread, worker.id, later)
        yield* store.subscriptions.subscribe(thread, second.id, later)
        yield* store.subscriptions.subscribe(otherThread, worker.id, later)
        yield* store.workers.update({ ...worker, state: "running", updatedAt: later })
        expect(yield* store.subscriptions.subscribers(thread)).toEqual([
          { workerId: worker.id, state: "running" },
          { workerId: second.id, state: "idle" },
        ])
        expect(yield* store.subscriptions.threadsOf(worker.id)).toEqual([thread, otherThread])
        yield* inspect(filename, (db) =>
          expect(
            db
              .prepare("SELECT created_at FROM subscriptions WHERE thread = ? AND worker_id = ?")
              .get(thread, worker.id),
          ).toMatchObject({ created_at: at }),
        )
        yield* store.subscriptions.unsubscribe(thread, worker.id)
        yield* store.subscriptions.unsubscribe(thread, worker.id)
        expect(yield* store.subscriptions.subscribers(thread)).toEqual([
          { workerId: second.id, state: "idle" },
        ])
        expect(yield* store.subscriptions.threadsOf(worker.id)).toEqual([otherThread])
        expect(yield* store.subscriptions.threadsOf(WorkerId.make("missing"))).toEqual([])
      }).pipe(Effect.provide(SqliteStoreLayer(filename))),
    ),
  )

  it.effect("records each delivery once, keeps its timestamp and completion, and increments attempts", () =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        const store = yield* Store
        yield* store.events.insert(event)
        yield* store.deliveries.record(event.id, "router", at)
        yield* store.deliveries.record(event.id, "router", later)
        yield* store.deliveries.record(event.id, `worker:${worker.id}`, later)
        expect(yield* store.deliveries.pending()).toEqual([
          { eventId: event.id, target: "router", status: "pending", attempts: 0, createdAt: at },
          {
            eventId: event.id,
            target: `worker:${worker.id}`,
            status: "pending",
            attempts: 0,
            createdAt: later,
          },
        ])
        yield* store.deliveries.markDone(event.id, "router")
        yield* store.deliveries.record(event.id, "router", later)
        yield* store.deliveries.markDone(event.id, "router")
        expect((yield* store.deliveries.pending()).map((item) => item.target)).toEqual([
          `worker:${worker.id}`,
        ])
        yield* inspect(filename, (db) =>
          expect(
            db.prepare("SELECT status, attempts, created_at FROM deliveries WHERE target = 'router'").get(),
          ).toMatchObject({ status: "done", attempts: 2, created_at: at }),
        )
        expect(
          (yield* store.deliveries.record(EventId.make("missing"), "router", at).pipe(Effect.flip)).operation,
        ).toBe("deliveries.record")
      }).pipe(Effect.provide(SqliteStoreLayer(filename))),
    ),
  )

  it.effect("keeps the first outbox entry and stores complete and failed results", () =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        const store = yield* Store
        const first = {
          operationId: op("reply"),
          kind: "slack_reply" as const,
          payload: { text: "hello 🐜", items: [null, 42, true] },
          at,
        }
        expect(yield* store.outbox.get(first.operationId)).toEqual(Option.none())
        yield* store.outbox.enqueue(first)
        yield* store.outbox.enqueue({ ...first, kind: "worker_stop", payload: "different", at: later })
        const original = {
          operationId: first.operationId,
          kind: first.kind,
          payload: first.payload,
          status: "pending",
          result: null,
          createdAt: at,
          updatedAt: at,
        }
        expect(yield* store.outbox.get(first.operationId)).toEqual(Option.some(original))
        const result = { team: "T-TEST", conversation: "C-TEST", ts: "100.004" }
        yield* store.outbox.complete(first.operationId, result, later)
        yield* store.outbox.enqueue(first)
        expect(yield* store.outbox.get(first.operationId)).toEqual(
          Option.some({ ...original, status: "done", result, updatedAt: later }),
        )
        yield* store.outbox.enqueue({ operationId: op("failed"), kind: "worker_launch", payload: null, at })
        yield* store.outbox.fail(op("failed"), "can't launch\nreason", later)
        yield* store.outbox.enqueue({
          operationId: op("failed"),
          kind: "worker_send",
          payload: {},
          at: later,
        })
        expect(yield* store.outbox.get(op("failed"))).toEqual(
          Option.some({
            operationId: op("failed"),
            kind: "worker_launch",
            payload: null,
            status: "failed",
            result: "can't launch\nreason",
            createdAt: at,
            updatedAt: later,
          }),
        )
        yield* store.outbox.enqueue({
          operationId: op("pending"),
          kind: "plan_upsert",
          payload: [],
          at: later,
        })
        expect((yield* store.outbox.pending()).map((item) => item.operationId)).toEqual(["pending"])
        yield* store.outbox.complete(op("missing"), {}, later)
        yield* store.outbox.fail(op("missing"), "reason", later)
        expect(yield* store.outbox.get(op("missing"))).toEqual(Option.none())
      }).pipe(Effect.provide(SqliteStoreLayer(filename))),
    ),
  )

  it.effect("rolls back every table when a transaction fails halfway and commits successful groups", () =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        const store = yield* Store
        const writes = Effect.gen(function* () {
          yield* store.events.insert(event)
          yield* store.events.markDispatched(event.id, later)
          yield* store.threads.touch(thread, at)
          yield* store.workers.insert(worker)
          yield* store.transitions.append({
            workerId: worker.id,
            from: "launching",
            to: "running",
            input: "launched",
            reason: null,
            at,
          })
          yield* store.subscriptions.subscribe(thread, worker.id, at)
          yield* store.deliveries.record(event.id, "router", at)
          yield* store.outbox.enqueue({ operationId: op("in-tx"), kind: "slack_reply", payload: {}, at })
          return "committed"
        })
        expect(
          yield* store.transaction(writes.pipe(Effect.andThen(Effect.fail("boom")))).pipe(Effect.flip),
        ).toBe("boom")
        yield* inspect(filename, (db) => {
          for (const table of [
            "events",
            "threads",
            "workers",
            "worker_transitions",
            "subscriptions",
            "deliveries",
            "outbox",
          ]) {
            expect(db.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get()).toMatchObject({ count: 0 })
          }
        })
        expect(yield* store.transaction(writes)).toBe("committed")
        expect(yield* store.events.get(event.id)).toEqual(Option.some(event))
        expect(yield* store.events.undispatched()).toEqual([])
        expect((yield* store.outbox.pending()).length).toBe(1)
        expect((yield* store.subscriptions.subscribers(thread)).length).toBe(1)
      }).pipe(Effect.provide(SqliteStoreLayer(filename))),
    ),
  )

  it.effect("nests transactions as savepoints: a caught inner failure undoes only its own writes", () =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        const store = yield* Store
        const enqueue = (id: string) =>
          store.outbox.enqueue({ operationId: op(id), kind: "slack_reply", payload: {}, at })
        const nested = Effect.gen(function* () {
          yield* enqueue("outer")
          yield* store
            .transaction(enqueue("inner").pipe(Effect.andThen(Effect.fail("inner failure"))))
            .pipe(Effect.ignore)
        })
        yield* store
          .transaction(nested.pipe(Effect.andThen(Effect.fail("outer failure"))))
          .pipe(Effect.ignore)
        expect(yield* store.outbox.pending()).toEqual([])
        yield* store.transaction(nested)
        expect((yield* store.outbox.pending()).map((entry) => entry.operationId)).toEqual(["outer"])
      }).pipe(Effect.provide(SqliteStoreLayer(filename))),
    ),
  )

  it.effect("restores existing rows when a SQL write fails halfway through a transaction", () =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        const store = yield* Store
        yield* store.events.insert(event)
        yield* store.workers.insert(worker)
        yield* store.subscriptions.subscribe(thread, worker.id, at)
        yield* store.deliveries.record(event.id, "router", at)
        yield* store.outbox.enqueue({ operationId: op("existing"), kind: "worker_launch", payload: {}, at })

        const error = yield* store
          .transaction(
            Effect.gen(function* () {
              yield* store.events.markDispatched(event.id, later)
              yield* store.workers.update({ ...worker, state: "running", updatedAt: later })
              yield* store.subscriptions.unsubscribe(thread, worker.id)
              yield* store.deliveries.markDone(event.id, "router")
              yield* store.outbox.complete(op("existing"), { launched: true }, later)
              yield* store.deliveries.record(EventId.make("missing"), "router", later)
            }),
          )
          .pipe(Effect.flip)

        expect(error.operation).toBe("deliveries.record")
        expect(yield* store.events.undispatched()).toEqual([event])
        expect(yield* store.workers.get(worker.id)).toEqual(Option.some(worker))
        expect(yield* store.subscriptions.subscribers(thread)).toEqual([
          { workerId: worker.id, state: "launching" },
        ])
        expect(yield* store.deliveries.pending()).toEqual([
          { eventId: event.id, target: "router", status: "pending", attempts: 0, createdAt: at },
        ])
        expect(yield* store.outbox.get(op("existing"))).toEqual(
          Option.some({
            operationId: op("existing"),
            kind: "worker_launch",
            payload: {},
            status: "pending",
            result: null,
            createdAt: at,
            updatedAt: at,
          }),
        )
      }).pipe(Effect.provide(SqliteStoreLayer(filename))),
    ),
  )

  it.effect("maps a failed commit to StoreError and rolls it back before reusing the connection", () =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        const store = yield* Store
        // A deferred constraint fails at COMMIT rather than at the repository write.
        yield* inspect(filename, (db) =>
          db.exec(`
          CREATE TABLE commit_failure (
            worker_id TEXT REFERENCES workers (id) DEFERRABLE INITIALLY DEFERRED
          );
          CREATE TRIGGER fail_commit AFTER INSERT ON outbox BEGIN
            INSERT INTO commit_failure (worker_id) VALUES ('missing');
          END;
        `),
        )
        const enqueue = store.outbox.enqueue({
          operationId: op("commit"),
          kind: "slack_reply",
          payload: {},
          at,
        })
        const error = yield* store.transaction(enqueue).pipe(Effect.flip)
        expect(error).toBeInstanceOf(StoreError)
        expect(error.operation).toBe("transaction")
        expect(yield* store.outbox.pending()).toEqual([])
        yield* inspect(filename, (db) => db.exec("DROP TRIGGER fail_commit"))
        yield* store.transaction(enqueue)
        expect((yield* store.outbox.pending()).map((entry) => entry.operationId)).toEqual(["commit"])
      }).pipe(Effect.provide(SqliteStoreLayer(filename))),
    ),
  )

  it.effect("rolls back an interrupted transaction and releases its connection", () =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        const store = yield* Store
        const wrote = yield* Deferred.make<void>()
        const transaction = yield* store
          .transaction(
            store.events
              .insert(event)
              .pipe(Effect.andThen(Deferred.succeed(wrote, undefined)), Effect.andThen(Effect.never)),
          )
          .pipe(Effect.forkChild)
        yield* Deferred.await(wrote)
        yield* Fiber.interrupt(transaction)
        expect(yield* store.events.get(event.id)).toEqual(Option.none())
        expect(yield* store.transaction(store.events.insert(event))).toEqual({ inserted: true, event })
      }).pipe(Effect.provide(SqliteStoreLayer(filename))),
    ),
  )

  it.effect(
    "serializes a lone insert racing a transaction and hides uncommitted writes from another connection",
    () =>
      withDatabase((filename) =>
        Effect.gen(function* () {
          const store = yield* Store
          const wrote = yield* Deferred.make<void>()
          const proceed = yield* Deferred.make<void>()
          const transaction = yield* store
            .transaction(
              store.events
                .insert(event)
                .pipe(
                  Effect.andThen(Deferred.succeed(wrote, undefined)),
                  Effect.andThen(Deferred.await(proceed)),
                ),
            )
            .pipe(Effect.forkChild)
          yield* Deferred.await(wrote)
          yield* inspect(filename, (db) =>
            expect(db.prepare("SELECT COUNT(*) AS count FROM events").get()).toMatchObject({ count: 0 }),
          )
          const competing = yield* store.events
            .insert({ ...event, id: EventId.make("retry") })
            .pipe(Effect.forkChild)
          yield* Deferred.succeed(proceed, undefined)
          yield* Fiber.join(transaction)
          expect(yield* Fiber.join(competing)).toEqual({ inserted: false, event })
          expect(yield* store.events.undispatched()).toEqual([event])
        }).pipe(Effect.provide(SqliteStoreLayer(filename))),
      ),
  )

  it.effect("maps open failures to StoreError", () =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        const error = yield* Effect.gen(function* () {
          yield* Store
        }).pipe(Effect.provide(SqliteStoreLayer(join(filename, "missing", "store.db"))), Effect.flip)
        expect(error).toBeInstanceOf(StoreError)
        expect(error.operation).toBe("open")
      }),
    ),
  )

  it.effect("fails loudly on invalid JSON and invalid domain fields read from the database", () =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        const store = yield* Store
        yield* store.events.insert(event)
        yield* store.workers.insert(worker)
        yield* store.outbox.enqueue({ operationId: op("bad"), kind: "slack_reply", payload: {}, at })
        for (const data of ["not JSON", JSON.stringify({ ...event, receivedAt: -1 })]) {
          yield* inspect(filename, (db) => db.prepare("UPDATE events SET data = ?").run(data))
          expect((yield* store.events.get(event.id).pipe(Effect.flip)).operation).toBe("events.get")
          expect((yield* store.events.undispatched().pipe(Effect.flip)).operation).toBe("events.undispatched")
          expect((yield* store.events.insert(event).pipe(Effect.flip)).operation).toBe("events.insert")
        }
        yield* inspect(filename, (db) => db.prepare("UPDATE workers SET brief = '{}' ").run())
        expect((yield* store.workers.get(worker.id).pipe(Effect.flip)).operation).toBe("workers.get")
        yield* inspect(filename, (db) =>
          db
            .prepare("UPDATE workers SET brief = ?, state = 'invalid'")
            .run(Schema.encodeSync(Schema.fromJsonString(Worker.fields.brief))(worker.brief)),
        )
        expect((yield* store.workers.get(worker.id).pipe(Effect.flip)).operation).toBe("workers.get")
        yield* store.subscriptions.subscribe(thread, worker.id, at)
        expect((yield* store.subscriptions.subscribers(thread).pipe(Effect.flip)).operation).toBe(
          "subscriptions.subscribers",
        )
        yield* inspect(filename, (db) => db.prepare("UPDATE outbox SET payload = 'broken'").run())
        expect((yield* store.outbox.get(op("bad")).pipe(Effect.flip)).operation).toBe("outbox.get")
        expect((yield* store.outbox.pending().pipe(Effect.flip)).operation).toBe("outbox.pending")
        expect(
          (yield* store.outbox
            .enqueue({ operationId: op("undefined"), kind: "slack_reply", payload: undefined, at })
            .pipe(Effect.flip)).operation,
        ).toBe("outbox.enqueue")
      }).pipe(Effect.provide(SqliteStoreLayer(filename))),
    ),
  )
})
