import { BACKGROUND_CONTEXT, withAbortSignal } from "@earendil-works/chord/context"
import type { Models } from "@earendil-works/pi-ai/models"
import { createModels } from "@earendil-works/pi-ai/models"
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai"
import { createRegistry, defineExtension, defineTool, Harness } from "@earendil-works/pi-durable"
import type { ToolExecutionResult } from "@earendil-works/pi-durable"
import { openNodeSqliteStorage } from "@earendil-works/pi-durable/storage/sqlite/node"
import { Effect, Layer, Schema } from "effect"
import type { AnyRouterTool } from "../../core/domain/tool.ts"
import { EngineError, RouterEngine, RouterTools } from "../../ports/RouterEngine.ts"
import { openCredentials } from "./credentials.ts"

export { login } from "./login.ts"

const Config = Schema.Struct({
  storagePath: Schema.NonEmptyString,
  credentialsDir: Schema.NonEmptyString,
  modelId: Schema.NonEmptyString,
  thinkingLevel: Schema.Literals(["off", "minimal", "low", "medium", "high", "xhigh"]),
  provider: Schema.optionalKey(Schema.NonEmptyString),
})
export type PiDurableRouterConfig = typeof Config.Type

const SubmissionInput = Schema.Struct({
  requestId: Schema.String,
  content: Schema.String,
  whenBusy: Schema.Literals(["queue", "steer"]),
})

const engineError = (operation: string) => (cause: unknown) =>
  new EngineError({ operation, message: String(cause) })

const wrapTool = (tool: AnyRouterTool) => {
  const document = Schema.toJsonSchemaDocument(tool.input)
  return defineTool({
    name: tool.name,
    description: tool.description,
    parameters: { ...document.schema, $defs: document.definitions },
    replay: tool.replaySafe ? "safe" : "unsafe",
    execute: (args, api, context) =>
      Effect.runPromise(
        Effect.gen(function* (): Effect.fn.Return<ToolExecutionResult, string> {
          const input = yield* Schema.decodeUnknownEffect(tool.input)(args).pipe(
            Effect.mapError((error) => String(error)),
          )
          const text = yield* tool
            .run(input, { id: api.callId })
            .pipe(Effect.mapError((error) => error.message))
          return {
            output: [{ type: "text", text }],
            ...(tool.endsRun ? { control: { terminate: true } } : {}),
          }
        }).pipe(
          Effect.catch((text) =>
            Effect.succeed<ToolExecutionResult>({ output: [{ type: "text", text }], isError: true }),
          ),
        ),
        { signal: context.abortSignal },
      ),
  })
}

/** The scope owns Pi's storage and scheduler. Inject Models to run offline providers in tests. */
export const PiDurableRouterEngineLayer = (
  config: PiDurableRouterConfig,
  options: { readonly models?: Models } = {},
): Layer.Layer<RouterEngine, EngineError, RouterTools> =>
  Layer.effect(
    RouterEngine,
    Effect.gen(function* () {
      const settings = yield* Schema.decodeUnknownEffect(Config)(config).pipe(
        Effect.mapError(engineError("config")),
      )
      const router = yield* RouterTools
      const models = yield* Effect.gen(function* () {
        if (options.models !== undefined) return options.models
        const { store } = yield* openCredentials(settings.credentialsDir)
        const collection = createModels({ credentials: store })
        collection.setProvider(openaiProvider())
        return collection
      })
      const installed = yield* Effect.try({
        try: () => {
          const tools = router.tools.map(wrapTool)
          const registry = createRegistry()
          registry.install(defineExtension({ name: "atta", tools }))
          return { registry, tools }
        },
        catch: engineError("tools"),
      })
      const storage = yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: () => openNodeSqliteStorage(settings.storagePath),
          catch: engineError("open"),
        }),
        (resource) =>
          Effect.tryPromise({
            try: () => resource.close(BACKGROUND_CONTEXT),
            catch: engineError("close"),
          }).pipe(Effect.orDie),
      )
      const harness = yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: () => Harness.open(storage, { models, registry: installed.registry }, BACKGROUND_CONTEXT),
          catch: engineError("open"),
        }),
        (resource) =>
          Effect.tryPromise({
            try: () => resource.close(BACKGROUND_CONTEXT),
            catch: engineError("close"),
          }).pipe(Effect.orDie),
      )
      const root = yield* Effect.tryPromise({
        try: () => harness.root(BACKGROUND_CONTEXT),
        catch: engineError("root"),
      })
      yield* Effect.tryPromise({
        try: () =>
          root.configure(
            {
              model: { provider: settings.provider ?? "openai", modelId: settings.modelId },
              thinkingLevel: settings.thinkingLevel,
              instructions: router.instructions,
              tools: installed.tools,
            },
            BACKGROUND_CONTEXT,
          ),
        catch: engineError("configure"),
      })
      yield* Effect.try({ try: () => harness.resume(), catch: engineError("resume") })

      return RouterEngine.of({
        submit: Effect.fn("RouterEngine.submit")(function* (input: typeof SubmissionInput.Type) {
          const decoded = yield* Schema.decodeUnknownEffect(SubmissionInput)(input).pipe(
            Effect.mapError(engineError("submit")),
          )
          yield* Effect.tryPromise({
            try: (signal) =>
              root.submit(
                { ...decoded, type: "input", whenBusy: decoded.whenBusy === "queue" ? "followUp" : "steer" },
                withAbortSignal(signal, BACKGROUND_CONTEXT),
              ),
            catch: engineError("submit"),
          })
        }),
        settled: Effect.fn("RouterEngine.settled")(function* (requestId: string) {
          return yield* Effect.tryPromise({
            try: async (signal) => {
              const context = withAbortSignal(signal, BACKGROUND_CONTEXT)
              const record = await root.commit((tx) => tx.submissionByRequest(root.id, requestId), context)
              if (record === undefined) return undefined
              const submission = await harness.submission(record.id, context)
              return submission?.wait(context)
            },
            catch: engineError("settled"),
          }).pipe(
            Effect.flatMap((record) =>
              record === undefined
                ? Effect.fail(
                    new EngineError({ operation: "settled", message: `Unknown request: ${requestId}` }),
                  )
                : Effect.succeed(
                    record.status === "done"
                      ? ({ status: "answered" } as const)
                      : ({ status: "unanswered", reason: record.reason } as const),
                  ),
            ),
          )
        }),
      })
    }),
  )
