import { describe, expect, it } from "@effect/vitest"
import { Deferred, Effect, Fiber, Layer, Option, Schema } from "effect"
import { EventId, OperationId, Timestamp, WorkerId } from "../src/core/domain/ids.ts"
import { defineRouterTool, ToolError } from "../src/core/domain/tool.ts"
import type { Step } from "../src/core/domain/worker.ts"
import { RouterEngine, RouterTools } from "../src/ports/RouterEngine.ts"
import { Runtime } from "../src/ports/Runtime.ts"
import { Store } from "../src/ports/Store.ts"
import { FakeRouterControl, FakeRouterEngineLayer } from "./fakes/FakeRouterEngine.ts"
import { FakeRuntimeControl, FakeRuntimeLayer } from "./fakes/FakeRuntime.ts"
import { FakeStoreLayer } from "./fakes/FakeStore.ts"

const at = Timestamp.make(1)
const event = {
  _tag: "WorkerReport" as const,
  id: EventId.make("e1"),
  source: "worker" as const,
  externalId: "r1",
  receivedAt: at,
  workerId: WorkerId.make("w1"),
  text: "done",
}
const op = (id: string) => OperationId.make(id)

describe("FakeStore", () => {
  it.effect("stores an event once per source and external ID, and lists it until dispatched", () =>
    Effect.gen(function* () {
      const store = yield* Store
      expect((yield* store.events.insert(event)).inserted).toBe(true)
      expect((yield* store.events.insert({ ...event, id: EventId.make("e2") })).inserted).toBe(false)
      expect((yield* store.events.undispatched()).map((e) => e.id)).toEqual(["e1"])
      yield* store.events.markDispatched(event.id, at)
      expect(yield* store.events.undispatched()).toEqual([])
    }).pipe(Effect.provide(FakeStoreLayer)),
  )

  it.effect("discards a failed transaction's writes", () =>
    Effect.gen(function* () {
      const store = yield* Store
      yield* store
        .transaction(
          store.outbox
            .enqueue({ operationId: op("op1"), kind: "slack_reply", payload: {}, at })
            .pipe(Effect.andThen(Effect.fail("boom"))),
        )
        .pipe(Effect.ignore)
      expect(Option.isNone(yield* store.outbox.get(op("op1")))).toBe(true)
    }).pipe(Effect.provide(FakeStoreLayer)),
  )

  it.effect("hides a transaction's writes until it commits, and keeps writes made while it ran", () =>
    Effect.gen(function* () {
      const store = yield* Store
      const wrote = yield* Deferred.make<void>()
      const proceed = yield* Deferred.make<void>()
      const tx = yield* store
        .transaction(
          store.outbox
            .enqueue({ operationId: op("in-tx"), kind: "slack_reply", payload: {}, at })
            .pipe(
              Effect.andThen(Deferred.succeed(wrote, undefined)),
              Effect.andThen(Deferred.await(proceed)),
              Effect.andThen(Effect.fail("boom")),
            ),
        )
        .pipe(Effect.ignore, Effect.forkChild)
      yield* Deferred.await(wrote)
      // Not visible outside before commit.
      expect(Option.isNone(yield* store.outbox.get(op("in-tx")))).toBe(true)
      // A lone write waits for the transaction instead of being lost when it rolls back.
      const lone = yield* store.outbox
        .enqueue({ operationId: op("outside"), kind: "slack_reply", payload: {}, at })
        .pipe(Effect.forkChild)
      yield* Deferred.succeed(proceed, undefined)
      yield* Fiber.join(tx)
      yield* Fiber.join(lone)
      expect(Option.isSome(yield* store.outbox.get(op("outside")))).toBe(true)
      expect(Option.isNone(yield* store.outbox.get(op("in-tx")))).toBe(true)
    }).pipe(Effect.provide(FakeStoreLayer)),
  )
})

describe("FakeRuntime", () => {
  const brief = { repo: "sandbox-repo", title: "t", text: "do it" }

  it.effect("returns the same worker for a repeated launch, without a branch until status", () =>
    Effect.gen(function* () {
      const runtime = yield* Runtime
      const a = yield* runtime.launch({ repo: "sandbox-repo", brief, operationId: op("op1") })
      const b = yield* runtime.launch({ repo: "sandbox-repo", brief, operationId: op("op1") })
      expect(b.runtimeRef).toBe(a.runtimeRef)
      expect(a.branch).toBeNull()
      expect((yield* runtime.status({ worker: a.runtimeRef, runId: a.runId })).assignedBranch).not.toBeNull()
    }).pipe(Effect.provide(FakeRuntimeLayer)),
  )

  it.effect("reads an unfinished step again until it finishes", () =>
    Effect.gen(function* () {
      const runtime = yield* Runtime
      const control = yield* FakeRuntimeControl
      const { runtimeRef, runId } = yield* runtime.launch({ repo: "r", brief, operationId: op("op") })
      const step = (id: string, position: number, status: Step["status"]): Step => ({
        id,
        position,
        runId,
        kind: "command",
        title: id,
        status,
        detail: null,
      })
      yield* control.putSteps(runtimeRef, [step("a", 1, "complete"), step("b", 2, "in_progress")])
      const first = yield* runtime.steps({ worker: runtimeRef, afterPosition: 0 })
      expect(first.nextPosition).toBe(1)
      yield* control.putSteps(runtimeRef, [step("b", 2, "complete"), step("c", 3, "complete")])
      const second = yield* runtime.steps({ worker: runtimeRef, afterPosition: first.nextPosition })
      expect(second.steps.map((s) => `${s.id}:${s.status}`)).toEqual(["b:complete", "c:complete"])
      expect(second.nextPosition).toBe(3)
      expect(yield* runtime.steps({ worker: runtimeRef, afterPosition: 3 })).toEqual({
        steps: [],
        nextPosition: 3,
      })
    }).pipe(Effect.provide(FakeRuntimeLayer)),
  )

  it.effect("keeps the status of the run atta started when the runtime starts its own", () =>
    Effect.gen(function* () {
      const runtime = yield* Runtime
      const control = yield* FakeRuntimeControl
      const { runtimeRef, runId } = yield* runtime.launch({ repo: "r", brief, operationId: op("op") })
      yield* control.finishRun(runtimeRef, runId, { outcome: "interrupted" })
      const continuation = yield* control.startContinuation(runtimeRef)
      expect((yield* runtime.status({ worker: runtimeRef, runId })).outcome).toBe("interrupted")
      expect((yield* runtime.status({ worker: runtimeRef, runId: continuation })).state).toBe("running")
    }).pipe(Effect.provide(FakeRuntimeLayer)),
  )
})

describe("FakeRouterEngine", () => {
  const echo = defineRouterTool({
    name: "echo",
    description: "Echoes text with the call ID",
    input: Schema.Struct({ text: Schema.String }),
    replaySafe: true,
    endsRun: false,
    run: ({ text }, call) =>
      text === "fail"
        ? Effect.fail(new ToolError({ tool: "echo", message: "no" }))
        : Effect.succeed(`${text}@${call.id}`),
  })
  const layer = FakeRouterEngineLayer.pipe(
    Layer.provide(Layer.succeed(RouterTools)({ instructions: "", tools: [echo] })),
  )

  it.effect("calls a registered tool with decoded arguments and a call ID", () =>
    Effect.gen(function* () {
      const control = yield* FakeRouterControl
      expect(yield* control.callTool("echo", { text: "hi" }, "c1")).toBe("hi@c1")
      const bad = yield* control.callTool("echo", { text: 1 }).pipe(Effect.flip)
      expect(bad._tag).toBe("ToolError")
    }).pipe(Effect.provide(layer)),
  )

  it.effect("waits to settle until the test settles, and rejects unknown requests", () =>
    Effect.gen(function* () {
      const engine = yield* RouterEngine
      const control = yield* FakeRouterControl
      yield* engine.submit({ requestId: "r1", content: "hi", whenBusy: "queue" })
      const waiting = yield* engine.settled("r1").pipe(Effect.forkChild)
      yield* control.settle("r1", { status: "unanswered", reason: "model_error" })
      expect(yield* Fiber.join(waiting)).toEqual({ status: "unanswered", reason: "model_error" })
      expect((yield* engine.settled("nope").pipe(Effect.flip))._tag).toBe("EngineError")
    }).pipe(Effect.provide(layer)),
  )
})
