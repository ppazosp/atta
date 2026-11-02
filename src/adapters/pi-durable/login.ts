import { createInterface } from "node:readline/promises"
import { stdin, stdout } from "node:process"
import { createModels } from "@earendil-works/pi-ai/models"
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai"
import { Console, Effect, Schema } from "effect"
import { EngineError } from "../../ports/RouterEngine.ts"
import { openCredentials } from "./credentials.ts"

/** Interactive ChatGPT sign-in; WP8 exposes this as atta login. */
export const login = Effect.fn("PiRouter.login")(function* (credentialsDir: string) {
  yield* Schema.decodeUnknownEffect(Schema.NonEmptyString)(credentialsDir).pipe(
    Effect.mapError(
      () => new EngineError({ operation: "login", message: "Credentials directory is required." }),
    ),
  )
  const { store, deviceId } = yield* openCredentials(credentialsDir)
  const models = createModels({ credentials: store })
  models.setProvider(openaiProvider())
  yield* Effect.tryPromise({
    try: (signal) =>
      models.login(
        "openai",
        "oauth",
        {
          signal,
          notify: (event) => {
            if (event.type === "auth_url") Effect.runSync(Console.log(event.url))
            if (event.type === "progress" || event.type === "info") Effect.runSync(Console.log(event.message))
          },
          prompt: async (prompt) => {
            const terminal = createInterface({ input: stdin, output: stdout })
            try {
              return await terminal.question(`${prompt.message} `, { signal: prompt.signal ?? signal })
            } finally {
              terminal.close()
            }
          },
        },
        { getDeviceId: () => deviceId, agentName: "atta" },
      ),
    // Provider errors may include the token response. Keep it out of diagnostics.
    catch: () =>
      new EngineError({ operation: "login", message: "ChatGPT sign-in failed. Try atta login again." }),
  })
})
