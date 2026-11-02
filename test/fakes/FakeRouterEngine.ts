import { Context, Effect, Layer, Ref, Schema } from "effect"
import { ToolError } from "../../src/core/domain/tool.ts"
import { RouterEngine, RouterTools } from "../../src/ports/RouterEngine.ts"

export interface Submission {
  readonly requestId: string
  readonly content: string
  readonly whenBusy: "queue" | "steer"
}

/**
 * Stands in for the router's model. It records what core submits, and lets the test play the model by calling
 * the tools core registered in `RouterTools`, with the arguments the model would send.
 */
export class FakeRouterControl extends Context.Service<
  FakeRouterControl,
  {
    readonly submissions: Effect.Effect<ReadonlyArray<Submission>>
    callTool(name: string, args: unknown): Effect.Effect<string, ToolError>
  }
>()("atta/test/FakeRouterControl") {}

export const FakeRouterEngineLayer = Layer.effectContext(
  Effect.gen(function* () {
    const { tools } = yield* RouterTools
    const submissions = yield* Ref.make<ReadonlyArray<Submission>>([])

    const engine = RouterEngine.of({
      submit: (input) =>
        Ref.update(submissions, (all) =>
          all.some((s) => s.requestId === input.requestId) ? all : [...all, input],
        ),
      settled: () => Effect.succeed({ status: "answered" as const }),
    })
    const control = FakeRouterControl.of({
      submissions: Ref.get(submissions),
      callTool: (name, args) =>
        Effect.gen(function* () {
          const tool = tools.find((t) => t.name === name)
          if (!tool) return yield* new ToolError({ tool: name, message: "no such tool" })
          const input = yield* Schema.decodeUnknownEffect(tool.input)(args).pipe(
            Effect.mapError((e) => new ToolError({ tool: name, message: e.message })),
          )
          return yield* tool.run(input)
        }),
    })
    return Context.make(RouterEngine, engine).pipe(Context.add(FakeRouterControl, control))
  }),
)
