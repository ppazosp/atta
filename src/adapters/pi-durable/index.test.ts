import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context"
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai/providers/faux"
import { createRegistry, Harness } from "@earendil-works/pi-durable"
import { openNodeSqliteStorage } from "@earendil-works/pi-durable/storage/sqlite/node"
import { createModels } from "@earendil-works/pi-ai/models"
import { getCurrentSystemPrompt, getCurrentTools } from "@earendil-works/pi-ai/utils/transcript"
import { describe, expect, it } from "@effect/vitest"
import { Deferred, Effect, Fiber, Layer, Schema } from "effect"
import { defineRouterTool, ToolError } from "../../core/domain/tool.ts"
import { EngineError, RouterEngine, RouterTools } from "../../ports/RouterEngine.ts"
import { PiDurableRouterEngineLayer, toolParameters } from "./index.ts"
import { input, temporaryDirectory, testEngine } from "./test-support.ts"

const reply = defineRouterTool({
  name: "reply",
  description: "Answer in the thread.",
  input: Schema.Struct({ text: Schema.String }),
  replaySafe: true,
  endsRun: true,
  run: ({ text }) => Effect.succeed(text),
})

describe("Pi Durable router", () => {
  it.effect("offers only atta tools, decodes arguments, and passes the model's call ID", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const calls: Array<{ count: number; id: string }> = []
      const tool = defineRouterTool({
        name: "count",
        description: "Read a count.",
        input: Schema.Struct({ count: Schema.NumberFromString }).annotate({ identifier: "Count" }),
        replaySafe: true,
        endsRun: false,
        run: ({ count }, { id }) =>
          Effect.sync(() => {
            calls.push({ count, id })
            return "counted"
          }),
      })
      const { layer, faux } = testEngine(
        directory,
        [tool],
        [
          (context, options, _state, model) => {
            expect(getCurrentTools(context.messages).map((offered) => offered.name)).toEqual(["count"])
            expect(JSON.stringify(getCurrentTools(context.messages)[0]?.parameters)).toContain("$defs")
            expect(getCurrentSystemPrompt(context.messages)).toContain("Only use atta tools.")
            expect(model.id).toBe("router-test")
            expect(options?.reasoning).toBe("medium")
            return fauxAssistantMessage(fauxToolCall("count", { count: "42" }, { id: "call-count" }), {
              stopReason: "toolUse",
            })
          },
          (context) => {
            expect(context.messages).toContainEqual(
              expect.objectContaining({
                role: "toolResult",
                toolCallId: "call-count",
                isError: false,
                content: [{ type: "text", text: "counted" }],
              }),
            )
            return fauxAssistantMessage("Done")
          },
        ],
        { storagePath: ":memory:" },
      )
      yield* Effect.gen(function* () {
        const engine = yield* RouterEngine
        yield* engine.submit(input("decode"))
        expect(yield* engine.settled("decode")).toEqual({ status: "answered" })
      }).pipe(Effect.provide(layer))
      expect(calls).toEqual([{ count: 42, id: "call-count" }])
      expect(faux.state.callCount).toBe(2)
    }).pipe(Effect.scoped),
  )

  it.effect("returns ToolError to the model and lets a failed ending tool continue", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const brokenReply = {
        ...reply,
        run: () => Effect.fail(new ToolError({ tool: "reply", message: "Channel is unavailable" })),
      }
      const { layer, faux } = testEngine(
        directory,
        [brokenReply],
        [
          fauxAssistantMessage(fauxToolCall("reply", { text: "Hello" }, { id: "failed-reply" }), {
            stopReason: "toolUse",
          }),
          (context) => {
            expect(context.messages).toContainEqual(
              expect.objectContaining({
                role: "toolResult",
                toolCallId: "failed-reply",
                isError: true,
                content: [{ type: "text", text: "Channel is unavailable" }],
              }),
            )
            return fauxAssistantMessage("Could not send the reply")
          },
        ],
      )
      yield* Effect.gen(function* () {
        const engine = yield* RouterEngine
        yield* engine.submit(input("tool-error"))
        expect(yield* engine.settled("tool-error")).toEqual({ status: "answered" })
      }).pipe(Effect.provide(layer))
      expect(faux.state.callCount).toBe(2)
    }).pipe(Effect.scoped),
  )

  it.effect("rejects invalid tool arguments without calling the handler", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const calls: string[] = []
      const { layer } = testEngine(
        directory,
        [
          {
            ...reply,
            run: () =>
              Effect.sync(() => {
                calls.push("reply")
                return "sent"
              }),
          },
        ],
        [
          fauxAssistantMessage(fauxToolCall("reply", {}), { stopReason: "toolUse" }),
          (context) => {
            expect(context.messages).toContainEqual(
              expect.objectContaining({ role: "toolResult", isError: true }),
            )
            return fauxAssistantMessage("Invalid input")
          },
        ],
      )
      yield* Effect.gen(function* () {
        const engine = yield* RouterEngine
        yield* engine.submit(input("invalid-tool"))
        expect(yield* engine.settled("invalid-tool")).toEqual({ status: "answered" })
      }).pipe(Effect.provide(layer))
      expect(calls).toEqual([])
    }).pipe(Effect.scoped),
  )

  it.effect("returns an Effect decoding failure to the model without running the tool", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      let calls = 0
      const tool = defineRouterTool({
        name: "count",
        description: "Decode a count.",
        input: Schema.Struct({ count: Schema.NumberFromString.check(Schema.isFinite()) }),
        replaySafe: true,
        endsRun: true,
        run: () =>
          Effect.sync(() => {
            calls++
            return "counted"
          }),
      })
      const { layer, faux } = testEngine(
        directory,
        [tool],
        [
          fauxAssistantMessage(fauxToolCall("count", { count: "not-a-number" }), { stopReason: "toolUse" }),
          (context) => {
            // The JSON Schema accepts strings; Effect must still decode the value.
            expect(context.messages).toContainEqual(
              expect.objectContaining({ role: "toolResult", isError: true }),
            )
            return fauxAssistantMessage("Invalid count")
          },
        ],
      )
      yield* Effect.gen(function* () {
        const engine = yield* RouterEngine
        yield* engine.submit(input("decode-error"))
        expect(yield* engine.settled("decode-error")).toEqual({ status: "answered" })
      }).pipe(Effect.provide(layer))
      expect(calls).toBe(0)
      expect(faux.state.callCount).toBe(2)
    }).pipe(Effect.scoped),
  )

  it.effect("ends a successful reply without a closing model turn and ignores repeated request IDs", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      let replies = 0
      const { layer, faux } = testEngine(
        directory,
        [
          {
            ...reply,
            run: () =>
              Effect.sync(() => {
                replies++
                return "sent"
              }),
          },
        ],
        [fauxAssistantMessage(fauxToolCall("reply", { text: "Hello" }), { stopReason: "toolUse" })],
      )
      yield* Effect.gen(function* () {
        const engine = yield* RouterEngine
        yield* engine.submit(input("repeat"))
        yield* engine.submit(input("repeat", "Different content must not be admitted", "steer"))
        expect(yield* engine.settled("repeat")).toEqual({ status: "answered" })
        yield* engine.submit(input("repeat"))
        expect(yield* engine.settled("repeat")).toEqual({ status: "answered" })
      }).pipe(Effect.provide(layer))
      expect(replies).toBe(1)
      expect(faux.state.callCount).toBe(1)
    }).pipe(Effect.scoped),
  )

  it.effect("places a steer after the current tool round and queues a follow-up for a new run", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const wait = defineRouterTool({
        name: "wait",
        description: "Wait for a result.",
        input: Schema.Struct({}),
        replaySafe: true,
        endsRun: false,
        run: () =>
          Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.as("result"),
          ),
      })
      const { layer, faux } = testEngine(
        directory,
        [wait],
        [
          fauxAssistantMessage(fauxToolCall("wait", {}), { stopReason: "toolUse" }),
          (context) => {
            expect(
              context.messages.filter((message) => message.role === "user").map((message) => message.content),
            ).toEqual(["first", "steer"])
            expect(context.messages.at(-2)?.role).toBe("toolResult")
            return fauxAssistantMessage("First and steer answered")
          },
          (context) => {
            expect(context.messages.at(-1)).toMatchObject({ role: "user", content: "queued" })
            return fauxAssistantMessage("Follow-up answered")
          },
        ],
      )
      yield* Effect.gen(function* () {
        const engine = yield* RouterEngine
        yield* engine.submit(input("first"))
        yield* Deferred.await(started)
        yield* engine.submit(input("queued"))
        yield* engine.submit(input("steer", "steer", "steer"))
        yield* Deferred.succeed(release, undefined)
        expect(yield* engine.settled("first")).toEqual({ status: "answered" })
        expect(yield* engine.settled("steer")).toEqual({ status: "answered" })
        expect(yield* engine.settled("queued")).toEqual({ status: "answered" })
      }).pipe(Effect.provide(layer))
      expect(faux.state.callCount).toBe(3)
    }).pipe(Effect.scoped),
  )

  it.effect("settles a missing model as unanswered and rejects unknown requests", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const { layer, faux } = testEngine(directory, [], [], { modelId: "missing-model" })
      yield* Effect.gen(function* () {
        const engine = yield* RouterEngine
        expect(yield* engine.settled("unknown").pipe(Effect.flip)).toBeInstanceOf(EngineError)
        yield* engine.submit(input("unanswered"))
        expect(yield* engine.settled("unanswered")).toEqual({ status: "unanswered", reason: "no_model" })
      }).pipe(Effect.provide(layer))
      expect(faux.state.callCount).toBe(0)
    }).pipe(Effect.scoped),
  )

  it.effect("cancels only a settlement wait, leaving the submission to finish", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const { layer } = testEngine(
        directory,
        [
          {
            ...reply,
            run: () =>
              Deferred.succeed(started, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.as("sent"),
              ),
          },
        ],
        [fauxAssistantMessage(fauxToolCall("reply", { text: "Hello" }), { stopReason: "toolUse" })],
      )
      yield* Effect.gen(function* () {
        const engine = yield* RouterEngine
        yield* engine.submit(input("cancel-wait"))
        yield* Deferred.await(started)
        const waiter = yield* engine.settled("cancel-wait").pipe(Effect.forkChild)
        yield* Fiber.interrupt(waiter)
        yield* Deferred.succeed(release, undefined)
        expect(yield* engine.settled("cancel-wait")).toEqual({ status: "answered" })
      }).pipe(Effect.provide(layer))
    }).pipe(Effect.scoped),
  )

  it.effect("resumes a safe unfinished tool on startup, retaining its call ID and request ID", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const started = yield* Deferred.make<void>()
      const callIds: string[] = []
      const interrupted = {
        ...reply,
        run: (_: { text: string }, call: { id: string }) =>
          Effect.gen(function* () {
            callIds.push(call.id)
            yield* Deferred.succeed(started, undefined)
            return yield* Effect.never
          }),
      }
      const first = testEngine(
        directory,
        [interrupted],
        [
          fauxAssistantMessage(fauxToolCall("reply", { text: "Hello" }, { id: "durable-call" }), {
            stopReason: "toolUse",
          }),
        ],
      )
      yield* Effect.gen(function* () {
        yield* (yield* RouterEngine).submit(input("resume"))
        yield* Deferred.await(started)
      }).pipe(Effect.provide(first.layer))
      const resumed = yield* Deferred.make<void>()
      const second = testEngine(
        directory,
        [
          {
            ...reply,
            run: (_: { text: string }, call: { id: string }) =>
              Effect.gen(function* () {
                callIds.push(call.id)
                yield* Deferred.succeed(resumed, undefined)
                return "sent"
              }),
          },
        ],
        [],
      )
      yield* Effect.gen(function* () {
        const engine = yield* RouterEngine
        // No submit or settled call starts this: layer startup must resume it.
        yield* Deferred.await(resumed)
        expect(yield* engine.settled("resume")).toEqual({ status: "answered" })
        yield* engine.submit(input("resume", "Must not submit again"))
        expect(yield* engine.settled("resume")).toEqual({ status: "answered" })
      }).pipe(Effect.provide(second.layer))
      expect(callIds).toEqual(["durable-call", "durable-call"])
      expect(first.faux.state.callCount).toBe(1)
      expect(second.faux.state.callCount).toBe(0)
      const harness = yield* Effect.acquireRelease(
        Effect.tryPromise(async () =>
          Harness.open(
            await openNodeSqliteStorage(first.storagePath),
            { registry: createRegistry(), models: createModels() },
            BACKGROUND_CONTEXT,
          ),
        ),
        (resource) => Effect.tryPromise(() => resource.close(BACKGROUND_CONTEXT)).pipe(Effect.orDie),
      )
      const root = yield* Effect.tryPromise(() => harness.root(BACKGROUND_CONTEXT))
      const record = yield* Effect.tryPromise(() =>
        root.commit((tx) => tx.submissionByRequest(root.id, "resume"), BACKGROUND_CONTEXT),
      )
      expect(record?.status).toBe("done")
      const entries = yield* Effect.tryPromise(() => root.entries({}, 100, undefined, BACKGROUND_CONTEXT))
      expect(entries.items.filter((entry) => entry.kind === "pi.user")).toHaveLength(1)
    }).pipe(Effect.scoped),
  )

  it.effect("maps opening and configuration failures to EngineError", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const badStorage = testEngine(directory, [], [], { storagePath: directory })
      expect(yield* Effect.void.pipe(Effect.provide(badStorage.layer), Effect.flip)).toMatchObject({
        _tag: "EngineError",
        operation: "open",
      })
      const invalidConfig = PiDurableRouterEngineLayer({
        storagePath: "",
        credentialsDir: directory,
        modelId: "none",
        thinkingLevel: "medium",
      }).pipe(Layer.provide(Layer.succeed(RouterTools, { instructions: "", tools: [] })))
      expect(yield* Effect.void.pipe(Effect.provide(invalidConfig), Effect.flip)).toMatchObject({
        _tag: "EngineError",
        operation: "config",
      })
    }).pipe(Effect.scoped),
  )

  it.effect("reports an interrupted unsafe tool to the model without repeating its handler", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const started = yield* Deferred.make<void>()
      let calls = 0
      const tool = {
        ...reply,
        replaySafe: false,
        run: () =>
          Effect.gen(function* () {
            calls++
            yield* Deferred.succeed(started, undefined)
            return yield* Effect.never
          }),
      }
      const first = testEngine(
        directory,
        [tool],
        [
          fauxAssistantMessage(fauxToolCall("reply", { text: "Hello" }, { id: "unsafe-call" }), {
            stopReason: "toolUse",
          }),
        ],
      )
      yield* Effect.gen(function* () {
        yield* (yield* RouterEngine).submit(input("unsafe"))
        yield* Deferred.await(started)
      }).pipe(Effect.provide(first.layer))
      const second = testEngine(
        directory,
        [tool],
        [
          (context) => {
            expect(context.messages).toContainEqual(
              expect.objectContaining({ role: "toolResult", toolCallId: "unsafe-call", isError: true }),
            )
            return fauxAssistantMessage("The reply was interrupted")
          },
        ],
      )
      yield* Effect.gen(function* () {
        expect(yield* (yield* RouterEngine).settled("unsafe")).toEqual({ status: "answered" })
      }).pipe(Effect.provide(second.layer))
      expect(calls).toBe(1)
      expect(second.faux.state.callCount).toBe(1)
    }).pipe(Effect.scoped),
  )
})

describe("tool parameters", () => {
  it("gives a tool without arguments an object schema, which OpenAI requires", () => {
    expect(toolParameters(Schema.Struct({}))).toEqual({
      type: "object",
      properties: {},
      additionalProperties: false,
    })
  })

  it("puts a named schema's object at the top", () => {
    const named = Schema.Struct({ count: Schema.Number }).annotate({ identifier: "Count" })
    const parameters = toolParameters(named)
    expect(parameters).toMatchObject({ type: "object", required: ["count"] })
    expect(parameters).not.toHaveProperty("$ref")
  })

  it("keeps an object schema as Effect renders it", () => {
    expect(toolParameters(Schema.Struct({ text: Schema.String }))).toMatchObject({
      type: "object",
      properties: { text: { type: "string" } },
      required: ["text"],
    })
  })
})
