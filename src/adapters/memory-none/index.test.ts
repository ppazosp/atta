import { describe, expect, it } from "@effect/vitest"
import { Effect } from "effect"
import { Memory } from "../../ports/Memory.ts"
import { MemoryNoneLayer } from "./index.ts"

describe("MemoryNoneLayer", () => {
  it.effect("recalls nothing and discards remembered facts", () =>
    Effect.gen(function* () {
      const memory = yield* Memory
      expect(yield* memory.recall("anything")).toBe("")
      expect(yield* memory.remember("a fact")).toBeUndefined()
      yield* memory.remember("another fact")
      expect(yield* memory.recall("a fact")).toBe("")
      expect(yield* memory.recall("")).toBe("")
    }).pipe(Effect.provide(MemoryNoneLayer)),
  )
})
