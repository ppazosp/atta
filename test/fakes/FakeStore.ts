import { Effect, Layer, Option, Ref } from "effect"
import type { Event } from "../../src/core/domain/event.ts"
import type { WorkerId } from "../../src/core/domain/ids.ts"
import type { ThreadKey } from "../../src/core/domain/thread.ts"
import type { Subscriber } from "../../src/core/domain/routing.ts"
import type { Worker } from "../../src/core/domain/worker.ts"
import type { Delivery, OutboxEntry, Transition } from "../../src/ports/Store.ts"
import { Store } from "../../src/ports/Store.ts"

interface State {
  readonly events: ReadonlyArray<Event>
  readonly threads: ReadonlyMap<string, number>
  readonly workers: ReadonlyMap<string, Worker>
  readonly transitions: ReadonlyArray<Transition>
  readonly subscriptions: ReadonlyArray<{ readonly thread: ThreadKey; readonly worker: WorkerId }>
  readonly deliveries: ReadonlyArray<Delivery>
  readonly outbox: ReadonlyMap<string, OutboxEntry>
}

const empty: State = {
  events: [],
  threads: new Map(),
  workers: new Map(),
  transitions: [],
  subscriptions: [],
  deliveries: [],
  outbox: new Map(),
}

/** An in-memory Store. A failed transaction restores the state it started from. */
export const makeFakeStore = Effect.gen(function* () {
  const state = yield* Ref.make(empty)
  const update = (f: (s: State) => State) => Ref.update(state, f)
  const read = <A>(f: (s: State) => A) => Ref.get(state).pipe(Effect.map(f))

  const store = Store.of({
    transaction: (effect) =>
      Effect.gen(function* () {
        const before = yield* Ref.get(state)
        return yield* effect.pipe(Effect.onError(() => Ref.set(state, before)))
      }),
    events: {
      insert: (event) =>
        Effect.gen(function* () {
          const existing = (yield* Ref.get(state)).events.find(
            (e) => e.source === event.source && e.externalId === event.externalId,
          )
          if (existing) return { inserted: false, event: existing }
          yield* update((s) => ({ ...s, events: [...s.events, event] }))
          return { inserted: true, event }
        }),
      get: (id) => read((s) => Option.fromNullishOr(s.events.find((e) => e.id === id))),
    },
    threads: {
      touch: (key, at) => update((s) => ({ ...s, threads: new Map(s.threads).set(key, at) })),
    },
    workers: {
      insert: (worker) => update((s) => ({ ...s, workers: new Map(s.workers).set(worker.id, worker) })),
      get: (id) => read((s) => Option.fromNullishOr(s.workers.get(id))),
      update: (worker) => update((s) => ({ ...s, workers: new Map(s.workers).set(worker.id, worker) })),
      inStates: (states) => read((s) => [...s.workers.values()].filter((w) => states.includes(w.state))),
    },
    transitions: {
      append: (transition) => update((s) => ({ ...s, transitions: [...s.transitions, transition] })),
      forWorker: (id) => read((s) => s.transitions.filter((t) => t.workerId === id)),
    },
    subscriptions: {
      subscribe: (thread, worker) =>
        update((s) =>
          s.subscriptions.some((x) => x.thread === thread && x.worker === worker)
            ? s
            : { ...s, subscriptions: [...s.subscriptions, { thread, worker }] },
        ),
      unsubscribe: (thread, worker) =>
        update((s) => ({
          ...s,
          subscriptions: s.subscriptions.filter((x) => !(x.thread === thread && x.worker === worker)),
        })),
      subscribers: (thread) =>
        read((s) =>
          s.subscriptions
            .filter((x) => x.thread === thread)
            .flatMap((x): Array<Subscriber> => {
              const worker = s.workers.get(x.worker)
              return worker ? [{ workerId: worker.id, state: worker.state }] : []
            }),
        ),
      threadsOf: (worker) =>
        read((s) => s.subscriptions.filter((x) => x.worker === worker).map((x) => x.thread)),
    },
    deliveries: {
      record: (eventId, target, at) =>
        update((s) =>
          s.deliveries.some((d) => d.eventId === eventId && d.target === target)
            ? s
            : {
                ...s,
                deliveries: [
                  ...s.deliveries,
                  { eventId, target, status: "pending", attempts: 0, createdAt: at },
                ],
              },
        ),
      markDone: (eventId, target) =>
        update((s) => ({
          ...s,
          deliveries: s.deliveries.map((d) =>
            d.eventId === eventId && d.target === target
              ? { ...d, status: "done", attempts: d.attempts + 1 }
              : d,
          ),
        })),
      pending: () => read((s) => s.deliveries.filter((d) => d.status === "pending")),
    },
    outbox: {
      enqueue: ({ operationId, kind, payload, at }) =>
        update((s) =>
          s.outbox.has(operationId)
            ? s
            : {
                ...s,
                outbox: new Map(s.outbox).set(operationId, {
                  operationId,
                  kind,
                  payload,
                  status: "pending",
                  result: null,
                  createdAt: at,
                  updatedAt: at,
                }),
              },
        ),
      get: (id) => read((s) => Option.fromNullishOr(s.outbox.get(id))),
      complete: (id, result, at) =>
        update((s) => {
          const entry = s.outbox.get(id)
          return entry
            ? { ...s, outbox: new Map(s.outbox).set(id, { ...entry, status: "done", result, updatedAt: at }) }
            : s
        }),
      fail: (id, reason, at) =>
        update((s) => {
          const entry = s.outbox.get(id)
          return entry
            ? {
                ...s,
                outbox: new Map(s.outbox).set(id, {
                  ...entry,
                  status: "failed",
                  result: reason,
                  updatedAt: at,
                }),
              }
            : s
        }),
      pending: () => read((s) => [...s.outbox.values()].filter((e) => e.status === "pending")),
    },
  })
  return { store, state }
})

export const FakeStoreLayer = Layer.effect(
  Store,
  Effect.map(makeFakeStore, ({ store }) => store),
)
