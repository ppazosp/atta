import type { Effect } from "effect"
import { Schema } from "effect"

export class ToolError extends Schema.TaggedError<ToolError>()("ToolError", {
  tool: Schema.String,
  message: Schema.String,
}) {}

/**
 * One of the router's tools. Core defines the tools (WP5); the router engine adapter (WP4) offers them to the
 * model, decodes the model's arguments with `input`, and runs `run`.
 */
export interface RouterTool<A = unknown> {
  readonly name: string
  readonly description: string
  /** Decodes the model's arguments. The engine also turns it into JSON Schema for the model. */
  readonly input: Schema.Decoder<A>
  /** Safe to run again after a crash cut it off. Unsafe tools report an interruption to the model instead. */
  readonly replaySafe: boolean
  /** Ends the router's run when it succeeds, so the model does not spend a turn on a closing remark. */
  readonly endsRun: boolean
  /** Returns the text the model sees as the tool result. */
  readonly run: (input: A) => Effect.Effect<string, ToolError>
}

/** Keeps the input type of `run` tied to the schema. */
export const defineRouterTool = <A>(tool: RouterTool<A>): RouterTool<A> => tool

/** A tool with its input type erased, for lists of tools. */
// The input type varies per tool; each tool decodes its own input.
export type AnyRouterTool = RouterTool<any>
