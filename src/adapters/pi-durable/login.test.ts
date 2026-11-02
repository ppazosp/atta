import type { LoginOptions, OAuthCredential, ProviderAuthInteraction } from "@earendil-works/pi-ai"
import * as OpenAI from "@earendil-works/pi-ai/providers/openai"
import { describe, expect, it } from "@effect/vitest"
import { Effect } from "effect"
import { vi } from "vitest"
import { openCredentials } from "./credentials.ts"
import { login } from "./index.ts"
import { temporaryDirectory } from "./test-support.ts"

vi.mock("@earendil-works/pi-ai/providers/openai", { spy: true })

describe("Pi ChatGPT login", () => {
  it.effect("prints the sign-in URL, passes the stable device ID, and persists the credential", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const deviceIds: Array<string | undefined> = []
      const credential: OAuthCredential = {
        type: "oauth",
        access: "fixture-access",
        refresh: "fixture-refresh",
        expires: 123,
        clientId: "fixture-client",
      }
      const provider = OpenAI.openaiProvider()
      const signIn = vi.mocked(OpenAI.openaiProvider).mockReturnValue({
        ...provider,
        auth: {
          oauth: {
            name: "ChatGPT",
            login: async (interaction: ProviderAuthInteraction, options?: LoginOptions) => {
              expect(options?.agentName).toBe("atta")
              deviceIds.push(options?.getDeviceId?.())
              interaction.notify({ type: "auth_url", url: "https://sign-in.example.test/fixture" })
              return credential
            },
            refresh: async () => credential,
            toAuth: async () => ({ apiKey: "fixture-access" }),
          },
        },
      })
      const printed = vi.spyOn(console, "log").mockImplementation(() => {})
      try {
        yield* login(directory)
        yield* login(directory)
        const persisted = yield* openCredentials(directory)
        expect(deviceIds).toEqual([persisted.deviceId, persisted.deviceId])
        expect(yield* Effect.tryPromise(() => persisted.store.read("openai"))).toEqual(credential)
        expect(printed).toHaveBeenCalledWith("https://sign-in.example.test/fixture")
        expect(printed.mock.calls.flat()).not.toContain("fixture-access")
      } finally {
        printed.mockRestore()
        signIn.mockRestore()
      }
    }).pipe(Effect.scoped),
  )

  it.effect("maps sign-in failures without exposing provider token responses", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const provider = OpenAI.openaiProvider()
      const signIn = vi.mocked(OpenAI.openaiProvider).mockReturnValue({
        ...provider,
        auth: {
          oauth: {
            name: "ChatGPT",
            login: () => Promise.reject(new Error("fixture-token-response")),
            refresh: () => Promise.reject(new Error("not used")),
            toAuth: async () => ({}),
          },
        },
      })
      try {
        const failure = yield* login(directory).pipe(Effect.flip)
        expect(failure).toMatchObject({ _tag: "EngineError", operation: "login" })
        expect(String(failure)).not.toContain("fixture-token-response")
      } finally {
        signIn.mockRestore()
      }
    }).pipe(Effect.scoped),
  )
})
