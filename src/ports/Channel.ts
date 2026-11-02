import type { Effect, Option, Stream } from "effect"
import { Context, Schema } from "effect"
import type { Event } from "../core/domain/event.ts"
import type { OperationId } from "../core/domain/ids.ts"
import type { Conversation, MessageRef, ThreadRef } from "../core/domain/thread.ts"
import type { RunOutcome, StepStatus } from "../core/domain/worker.ts"

export class ChannelError extends Schema.TaggedError<ChannelError>()("ChannelError", {
  operation: Schema.String,
  message: Schema.String,
}) {}

/** Reply in an existing thread, or start a new thread in a conversation (scheduled prompts). */
export type ReplyTarget = { readonly thread: ThreadRef } | { readonly conversation: Conversation }

export interface PlanTask {
  readonly id: string
  readonly title: string
  readonly status: StepStatus
  readonly detail: string | null
}

/** Slack, two ways: messages in, replies, reactions and plan messages out. */
export class Channel extends Context.Service<
  Channel,
  {
    /** DMs, mentions and replies in threads atta is part of. Only `UserMessage` events. */
    readonly events: Stream.Stream<Extract<Event, { _tag: "UserMessage" }>, ChannelError>
    /** Posts a message tagged with `operationId`. Returns the message; for a new thread it is the thread root. */
    reply(input: {
      readonly target: ReplyTarget
      readonly text: string
      readonly operationId: OperationId
    }): Effect.Effect<MessageRef, ChannelError>
    /** Finds a message `reply` already posted with this operation ID, for recovery after a crash. */
    findReply(input: {
      readonly thread: ThreadRef
      readonly operationId: OperationId
    }): Effect.Effect<Option.Option<MessageRef>, ChannelError>
    react(message: MessageRef, emoji: string): Effect.Effect<void, ChannelError>
    unreact(message: MessageRef, emoji: string): Effect.Effect<void, ChannelError>
    /**
     * Posts the plan message the first time (`plan` is null), then edits it. The adapter limits edits to about one
     * a second and groups tasks above Slack's limit, so callers can send every change.
     */
    upsertPlan(input: {
      readonly thread: ThreadRef
      readonly plan: MessageRef | null
      readonly title: string
      readonly tasks: ReadonlyArray<PlanTask>
      readonly operationId: OperationId
    }): Effect.Effect<MessageRef, ChannelError>
    /** The last edit of a plan message: final task states, the outcome and the worker's answer. */
    finishPlan(input: {
      readonly plan: MessageRef
      readonly title: string
      readonly tasks: ReadonlyArray<PlanTask>
      readonly outcome: RunOutcome
      readonly answer: string | null
    }): Effect.Effect<void, ChannelError>
  }
>()("atta/ports/Channel") {}
