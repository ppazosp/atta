import { describe, expect, it } from "@effect/vitest"
import { Effect, Layer, Option, Schema } from "effect"
import { EventId, OperationId, Timestamp } from "../src/core/domain/ids.ts"
import { defineRouterTool, ToolError } from "../src/core/domain/tool.ts"
import { RouterTools } from "../src/ports/RouterEngine.ts"
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
  workerId: "w1" as never,
  text: "done",
}

describe("FakeStore", () => {
  it.effect("stores an event once per source and external ID", () =>
    Effect.gen(function* () {
      const store = yield* Store
      expect((yield* store.events.insert(event)).inserted).toBe(true)
      expect((yield* store.events.insert({ ...event, id: EventId.make("e2") })).inserted).toBe(false)
    }).pipe(Effect.provide(FakeStoreLayer)),
  )

  it.effect("rolls back a failed transaction", () =>
    Effect.gen(function* () {
      const store = yield* Store
      const op = OperationId.make("op1")
      yield* store
        .transaction(
          store.outbox
            .enqueue({ operationId: op, kind: "slack_reply", payload: {}, at })
            .pipe(Effect.andThen(Effect.fail("boom"))),
        )
        .pipe(Effect.ignore)
      expect(Option.isNone(yield* store.outbox.get(op))).toBe(true)
    }).pipe(Effect.provide(FakeStoreLayer)),
  )
})

describe("FakeRuntime", () => {
  it.effect("returns the same worker for a repeated launch", () =>
    Effect.gen(function* () {
      const runtime = yield* Runtime
      const brief = { repo: "sandbox-repo", title: "t", text: "do it" }
      const a = yield* runtime.launch({ repo: "sandbox-repo", brief, operationId: OperationId.make("op1") })
      const b = yield* runtime.launch({ repo: "sandbox-repo", brief, operationId: OperationId.make("op1") })
      expect(b.runtimeRef).toBe(a.runtimeRef)
      expect((yield* (yield* FakeRuntimeControl).calls).length).toBe(2)
    }).pipe(Effect.provide(FakeRuntimeLayer)),
  )

  it.effect("reads steps after a position and keeps the position when nothing is new", () =>
    Effect.gen(function* () {
      const runtime = yield* Runtime
      const control = yield* FakeRuntimeControl
      const brief = { repo: "r", title: "t", text: "x" }
      const { runtimeRef, runId } = yield* runtime.launch({
        repo: "r",
        brief,
        operationId: OperationId.make("op"),
      })
      yield* control.addSteps(runtimeRef, [
        { position: 1, runId, kind: "command", title: "pnpm test", status: "complete", detail: null },
      ])
      expect((yield* runtime.steps({ worker: runtimeRef, afterPosition: 0 })).nextPosition).toBe(1)
      expect(yield* runtime.steps({ worker: runtimeRef, afterPosition: 1 })).toEqual({
        steps: [],
        nextPosition: 1,
      })
    }).pipe(Effect.provide(FakeRuntimeLayer)),
  )
})

describe("FakeRouterEngine", () => {
  const echo = defineRouterTool({
    name: "echo",
    description: "Echoes text",
    input: Schema.Struct({ text: Schema.String }),
    replaySafe: true,
    endsRun: false,
    run: ({ text }) =>
      text === "fail" ? Effect.fail(new ToolError({ tool: "echo", message: "no" })) : Effect.succeed(text),
  })
  const layer = FakeRouterEngineLayer.pipe(
    Layer.provide(Layer.succeed(RouterTools)({ instructions: "", tools: [echo] })),
  )

  it.effect("calls a registered tool with decoded arguments", () =>
    Effect.gen(function* () {
      const control = yield* FakeRouterControl
      expect(yield* control.callTool("echo", { text: "hi" })).toBe("hi")
      const bad = yield* control.callTool("echo", { text: 1 }).pipe(Effect.flip)
      expect(bad._tag).toBe("ToolError")
    }).pipe(Effect.provide(layer)),
  )
})
