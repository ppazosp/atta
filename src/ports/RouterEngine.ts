import type { Effect } from "effect"
import { Context, Schema } from "effect"
import type { AnyRouterTool } from "../core/domain/tool.ts"

export class EngineError extends Schema.TaggedError<EngineError>()("EngineError", {
  operation: Schema.String,
  message: Schema.String,
}) {}

export type Settlement =
  | { readonly status: "answered" }
  | { readonly status: "unanswered"; readonly reason: string }

/** The router's agent loop. It keeps one long-lived conversation and runs the tools from `RouterTools`. */
export class RouterEngine extends Context.Service<
  RouterEngine,
  {
    /**
     * Gives the router one input. A repeated `requestId` returns the existing submission, so redelivery after a
     * crash is safe. While the router is busy the input queues, or with `steer` joins the running turn.
     */
    submit(input: {
      readonly requestId: string
      readonly content: string
      readonly whenBusy: "queue" | "steer"
    }): Effect.Effect<void, EngineError>
    /** Waits until the input with `requestId` is answered or given up. */
    settled(requestId: string): Effect.Effect<Settlement, EngineError>
  }
>()("atta/ports/RouterEngine") {}

/** What the router runs with. Core provides it (WP5); the engine adapter reads it (WP4). */
export class RouterTools extends Context.Service<
  RouterTools,
  {
    readonly instructions: string
    readonly tools: ReadonlyArray<AnyRouterTool>
  }
>()("atta/ports/RouterTools") {}

/**
 * A second router conversation, only for messages observed in watched channels. Its text comes from anyone, so
 * it runs apart from the router, with tools that can propose work but never post, steer or start it.
 */
export class ObserverEngine extends Context.Service<ObserverEngine, RouterEngine["Service"]>()(
  "atta/ports/ObserverEngine",
) {}

/** What the observer runs with. Core provides it next to `RouterTools`. */
export class ObserverTools extends Context.Service<ObserverTools, RouterTools["Service"]>()(
  "atta/ports/ObserverTools",
) {}
