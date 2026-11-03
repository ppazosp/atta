import { Context, Deferred, Effect, Layer, Ref, Schema } from "effect"
import { ToolError } from "../../src/core/domain/tool.ts"
import { EngineError, RouterEngine, RouterTools, type Settlement } from "../../src/ports/RouterEngine.ts"

export interface Submission {
  readonly requestId: string
  readonly content: string
  readonly whenBusy: "queue" | "steer"
}

/**
 * Stands in for the router's model. It records what core submits, and lets the test play the model: call the
 * tools core registered in `RouterTools`, then settle the submission. `settled` waits until the test settles,
 * like the real engine waits for the model.
 */
export class FakeRouterControl extends Context.Service<
  FakeRouterControl,
  {
    readonly submissions: Effect.Effect<ReadonlyArray<Submission>>
    /**
     * Plays the model calling a tool. Pass the same `callId` to replay a call after a crash. `input` is the newest
     * input the model had read; by default the newest submission, as with the real engine.
     */
    callTool(
      name: string,
      args: unknown,
      callId?: string,
      input?: string | null,
    ): Effect.Effect<string, ToolError>
    settle(requestId: string, settlement: Settlement): Effect.Effect<void>
  }
>()("atta/test/FakeRouterControl") {}

const makeFakeEngine = (tools: RouterTools["Service"]["tools"]) =>
  Effect.gen(function* () {
    const submissions = yield* Ref.make<ReadonlyArray<Submission>>([])
    const settlements = yield* Ref.make<ReadonlyMap<string, Deferred.Deferred<Settlement>>>(new Map())
    const callCount = yield* Ref.make(0)

    const engine: RouterEngine["Service"] = {
      submit: (input) =>
        Effect.gen(function* () {
          if ((yield* Ref.get(settlements)).has(input.requestId)) return
          const deferred = yield* Deferred.make<Settlement>()
          yield* Ref.update(settlements, (all) => new Map(all).set(input.requestId, deferred))
          yield* Ref.update(submissions, (all) => [...all, input])
        }),
      settled: (requestId) =>
        Effect.gen(function* () {
          const deferred = (yield* Ref.get(settlements)).get(requestId)
          if (!deferred) {
            return yield* new EngineError({ operation: "settled", message: `unknown request ${requestId}` })
          }
          return yield* Deferred.await(deferred)
        }),
    }

    const control: FakeRouterControl["Service"] = {
      submissions: Ref.get(submissions),
      callTool: (name, args, callId, input) =>
        Effect.gen(function* () {
          const tool = tools.find((t) => t.name === name)
          if (!tool) return yield* new ToolError({ tool: name, message: "no such tool" })
          const decoded = yield* Schema.decodeUnknownEffect(tool.input)(args).pipe(
            Effect.mapError((e) => new ToolError({ tool: name, message: e.message })),
          )
          const id = callId ?? `call-${yield* Ref.updateAndGet(callCount, (n) => n + 1)}`
          // Like the real engine: the newest submission is the input the call's turn answers.
          const latest = input === undefined ? ((yield* Ref.get(submissions)).at(-1)?.content ?? null) : input
          return yield* tool.run(decoded, { id, input: latest })
        }),
      settle: (requestId, settlement) =>
        Effect.gen(function* () {
          const deferred = (yield* Ref.get(settlements)).get(requestId)
          if (!deferred) return yield* Effect.die(`unknown request ${requestId}`)
          yield* Deferred.succeed(deferred, settlement)
        }),
    }
    return { engine, control }
  })

export const FakeRouterEngineLayer = Layer.effectContext(
  Effect.gen(function* () {
    const { engine, control } = yield* makeFakeEngine((yield* RouterTools).tools)
    return Context.make(RouterEngine, engine).pipe(Context.add(FakeRouterControl, control))
  }),
)
