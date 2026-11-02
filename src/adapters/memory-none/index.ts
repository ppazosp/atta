import { Effect, Layer } from "effect"
import { Memory } from "../../ports/Memory.ts"

export const MemoryNoneLayer = Layer.succeed(Memory)(
  Memory.of({
    recall: Effect.fn("Memory.recall")((_query: string) => Effect.succeed("")),
    remember: Effect.fn("Memory.remember")((_fact: string) => Effect.void),
  }),
)
