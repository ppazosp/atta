import { Effect, Layer } from "effect"
import { Memory } from "../../src/ports/Memory.ts"

/** Knows nothing, like v0's placeholder memory. */
export const FakeMemoryLayer = Layer.succeed(Memory)(
  Memory.of({
    recall: () => Effect.succeed(""),
    remember: () => Effect.void,
  }),
)
