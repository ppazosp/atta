import { describe, expect, it } from "@effect/vitest"
import { Effect } from "effect"
import { version } from "../src/app/version.ts"

describe("version", () => {
  it.effect("is a semantic version", () =>
    Effect.sync(() => {
      expect(version).toMatch(/^\d+\.\d+\.\d+$/)
    }),
  )
})
