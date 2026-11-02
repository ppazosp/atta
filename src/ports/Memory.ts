import type { Effect } from "effect"
import { Context, Schema } from "effect"

export class MemoryError extends Schema.TaggedError<MemoryError>()("MemoryError", {
  message: Schema.String,
}) {}

/** What atta knows beyond the current conversation. v0 uses a placeholder that knows nothing. */
export class Memory extends Context.Service<
  Memory,
  {
    recall(query: string): Effect.Effect<string, MemoryError>
    remember(fact: string): Effect.Effect<void, MemoryError>
  }
>()("atta/ports/Memory") {}
