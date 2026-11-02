import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { join } from "node:path"
import type { FauxResponseStep } from "@earendil-works/pi-ai/providers/faux"
import { fauxProvider } from "@earendil-works/pi-ai/providers/faux"
import { createModels } from "@earendil-works/pi-ai/models"
import { Effect, Layer } from "effect"
import type { AnyRouterTool } from "../../core/domain/tool.ts"
import { RouterTools } from "../../ports/RouterEngine.ts"
import { PiDurableRouterEngineLayer } from "./index.ts"

export const temporaryDirectory = Effect.gen(function* () {
  const parent = join(process.cwd(), "data")
  yield* Effect.tryPromise(() => mkdir(parent, { recursive: true }))
  return yield* Effect.acquireRelease(
    Effect.tryPromise(() => mkdtemp(join(parent, "pi-durable-test-"))),
    (path) => Effect.tryPromise(() => rm(path, { recursive: true, force: true })).pipe(Effect.orDie),
  )
})

export const testEngine = (
  directory: string,
  tools: ReadonlyArray<AnyRouterTool>,
  responses: FauxResponseStep[],
  options: {
    readonly storagePath?: string
    readonly modelId?: string
  } = {},
) => {
  const faux = fauxProvider({ models: [{ id: "router-test", reasoning: true }] })
  faux.setResponses(responses)
  const models = createModels()
  models.setProvider(faux.provider)
  const storagePath = options.storagePath ?? join(directory, "router.sqlite")
  const layer = PiDurableRouterEngineLayer(
    {
      storagePath,
      credentialsDir: join(directory, "credentials"),
      modelId: options.modelId ?? "router-test",
      thinkingLevel: "medium",
      provider: "faux",
    },
    { models },
  ).pipe(Layer.provide(Layer.succeed(RouterTools, { instructions: "Only use atta tools.", tools })))
  return { layer, faux, storagePath }
}

export const input = (requestId: string, content = requestId, whenBusy: "queue" | "steer" = "queue") => ({
  requestId,
  content,
  whenBusy,
})
