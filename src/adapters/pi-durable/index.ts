import { BACKGROUND_CONTEXT, withAbortSignal } from "@earendil-works/chord/context"
import type { Context } from "@earendil-works/chord/context"
import type { Models } from "@earendil-works/pi-ai/models"
import { createModels } from "@earendil-works/pi-ai/models"
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai"
import { createRegistry, defineExtension, defineTool, Harness, UserEntry } from "@earendil-works/pi-durable"
import type { ToolExecutionApi, ToolExecutionResult, Tx } from "@earendil-works/pi-durable"
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

/**
 * OpenAI rejects a whole request ("response protection is unavailable") when any tool's parameters are not an
 * object schema. Effect renders an empty struct as `{ not: { type: "null" } }`, so a tool without arguments
 * gets an empty object schema instead.
 */
export const toolParameters = (input: AnyRouterTool["input"]) => {
  const document = Schema.toJsonSchemaDocument(input)
  const definitions = document.definitions as Record<string, Record<string, unknown>>
  let schema = document.schema as Record<string, unknown>
  // A named schema comes out as a reference to its definition; the model needs the object itself at the top.
  const ref = schema["$ref"]
  if (typeof ref === "string" && ref.startsWith("#/$defs/"))
    schema = definitions[ref.slice("#/$defs/".length)] ?? schema
  if (schema["type"] !== "object") return { type: "object", properties: {}, additionalProperties: false }
  return Object.keys(definitions).length === 0 ? schema : { ...schema, $defs: definitions }
}

/** Entries per page, and pages, a tool reads back to find the input its turn answers. */
const INPUT_PAGE = 100
const INPUT_PAGES = 20

/**
 * The text of the newest input in the calling conversation: the event the turn answers, or a later steer. Only
 * input entries count; a compaction summary or a reset's handoff is also a user message, but atta did not write it.
 */
export const latestInput = async (api: ToolExecutionApi, context: Context): Promise<string | null> => {
  let cursor: Parameters<Tx["scanEntries"]>[2]
  for (let page = 0; page < INPUT_PAGES; page++) {
    const result = await api.commit(
      (tx) => tx.scanEntries({ conversationId: api.conversationId, order: "descending" }, INPUT_PAGE, cursor),
      context,
    )
    for (const entry of result.items) {
      if (!UserEntry.is(entry)) continue
      const message = entry.model?.findLast((m) => m.role === "user")
      if (message === undefined) continue
      return typeof message.content === "string"
        ? message.content
        : message.content.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("\n")
    }
    if (result.next === undefined) return null
    cursor = result.next
  }
  return null
}

const wrapTool = (tool: AnyRouterTool) => {
  return defineTool({
    name: tool.name,
    description: tool.description,
    parameters: toolParameters(tool.input),
    replay: tool.replaySafe ? "safe" : "unsafe",
    execute: (args, api, context) =>
      Effect.runPromise(
        Effect.gen(function* (): Effect.fn.Return<ToolExecutionResult, string> {
          const input = yield* Schema.decodeUnknownEffect(tool.input)(args).pipe(
            Effect.mapError((error) => String(error)),
          )
          const latest = yield* Effect.tryPromise({
            try: () => latestInput(api, context),
            catch: (error) => `Could not read the conversation: ${String(error)}`,
          })
          const text = yield* tool
            .run(input, { id: api.callId, input: latest })
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

interface EngineOptions {
  readonly models?: Models
}

/** The router's conversation. The scope owns Pi's storage and scheduler. Inject Models to run offline in tests. */
export const PiDurableRouterEngineLayer = (
  config: PiDurableRouterConfig,
  options: EngineOptions = {},
): Layer.Layer<RouterEngine, EngineError, RouterTools> =>
  Layer.effect(
    RouterEngine,
    Effect.flatMap(RouterTools, (tools) => makeEngine(config, tools, options)),
  )

const makeEngine = (config: PiDurableRouterConfig, router: RouterTools["Service"], options: EngineOptions) =>
  Effect.gen(function* () {
    const settings = yield* Schema.decodeUnknownEffect(Config)(config).pipe(
      Effect.mapError(engineError("config")),
    )
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
  })
