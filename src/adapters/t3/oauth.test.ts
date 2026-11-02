import { readFile, stat, writeFile } from "node:fs/promises"
import { join } from "node:path"
import { describe, expect, it } from "@effect/vitest"
import { Effect } from "effect"
import { fileOAuthProvider } from "./index.ts"
import { temporaryDirectory } from "./test-support.ts"

describe("T3 OAuth provider", () => {
  it.effect("reads and atomically saves tokens and client registration in the configured directory", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const provider = fileOAuthProvider(directory)
      expect(yield* Effect.tryPromise(async () => provider.tokens())).toBeUndefined()
      expect(yield* Effect.tryPromise(async () => provider.clientInformation())).toBeUndefined()
      const initial = {
        access_token: "fixture-initial",
        refresh_token: "fixture-refresh",
        token_type: "Bearer",
        issuer: "https://t3.example.test",
      }
      yield* Effect.tryPromise(async () => provider.saveTokens(initial))
      const refreshed = { ...initial, access_token: "fixture-refreshed" }
      yield* Effect.tryPromise(async () => provider.saveTokens(refreshed))
      yield* Effect.tryPromise(async () =>
        provider.saveClientInformation!({ client_id: "fixture-client", issuer: "https://t3.example.test" }),
      )
      const reloaded = fileOAuthProvider(directory)
      expect(yield* Effect.tryPromise(async () => reloaded.tokens())).toEqual(refreshed)
      expect(yield* Effect.tryPromise(async () => reloaded.clientInformation())).toEqual({
        client_id: "fixture-client",
        issuer: "https://t3.example.test",
      })
      for (const filename of ["tokens.json", "client.json"]) {
        const info = yield* Effect.tryPromise(() => stat(join(directory, filename)))
        expect(info.mode & 0o777).toBe(0o600)
        const contents = yield* Effect.tryPromise(() => readFile(join(directory, filename), "utf8"))
        expect(contents.startsWith("{")).toBe(true)
      }
    }).pipe(Effect.scoped),
  )

  it.effect.each(["not json", '{"access_token":"secret-fixture-value","token_type":12}'])(
    "rejects invalid tokens without exposing them %#",
    (contents) =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory
        yield* Effect.tryPromise(() => writeFile(join(directory, "tokens.json"), contents))
        const failure = yield* Effect.tryPromise(async () => fileOAuthProvider(directory).tokens()).pipe(
          Effect.flip,
        )
        expect(String(failure.cause)).toContain("Invalid T3 OAuth tokens.json")
        expect(String(failure.cause)).not.toContain("secret-fixture-value")
      }).pipe(Effect.scoped),
  )

  it.effect("refuses interactive authorization", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const provider = fileOAuthProvider(directory)
      const failure = yield* Effect.tryPromise(async () =>
        provider.redirectToAuthorization(new URL("https://t3.example.test/authorize")),
      ).pipe(Effect.flip)
      expect(String(failure.cause)).toContain("Sign in to T3 with full access")
    }).pipe(Effect.scoped),
  )
})
