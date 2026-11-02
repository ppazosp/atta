import { describe, expect, it } from "@effect/vitest"
import { Effect, Schema } from "effect"
import example from "../../../atta.config.example.ts"
import { AttaConfig } from "./config.ts"

describe("config", () => {
  it.effect("accepts the example config", () =>
    Effect.gen(function* () {
      const config = yield* Schema.decodeUnknownEffect(AttaConfig)(example)
      expect(config.repos[0]?.name).toBe("sandbox-repo")
    }),
  )

  it.effect("rejects a port below 1024", () =>
    Effect.gen(function* () {
      const result = yield* Schema.decodeUnknownEffect(AttaConfig)({ ...example, mcp: { port: 80 } }).pipe(
        Effect.result,
      )
      expect(result._tag).toBe("Failure")
    }),
  )
})
