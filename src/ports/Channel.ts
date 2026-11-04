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

/** One message read back from Slack. `userId` is null for messages from bots, atta included. */
export interface ReadMessage {
  readonly userId: string | null
  readonly ts: string
  readonly text: string
}

/** A worker's request as posted: the message, and the Slack files its images were uploaded as. */
export interface AskMessage {
  readonly message: MessageRef
  readonly files: ReadonlyArray<string>
}

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
    /**
     * DMs, mentions, replies in threads, and every message in channels atta is in (`via: "channel"`; core
     * decides which channels it watches). Also clicks on proposals, and answers to workers' requests.
     */
    readonly events: Stream.Stream<
      Extract<Event, { _tag: "UserMessage" | "ApprovalDecision" | "WorkerAnswer" }>,
      ChannelError
    >
    /** Posts a message tagged with `operationId`. Returns the message; for a new thread it is the thread root. */
    reply(input: {
      readonly target: ReplyTarget
      readonly text: string
      readonly operationId: OperationId
    }): Effect.Effect<MessageRef, ChannelError>
    /**
     * Finds a message `reply` already posted with this operation ID, for recovery after a crash. For a new thread
     * the adapter searches the conversation's recent history; for a thread, the thread's replies.
     */
    findReply(input: {
      readonly target: ReplyTarget
      readonly operationId: OperationId
    }): Effect.Effect<Option.Option<MessageRef>, ChannelError>
    /**
     * Reads a thread, or the latest messages of a conversation, oldest first: at most `limit`, the newest ones.
     * Slack refuses conversations atta is not a member of.
     */
    readConversation(input: {
      readonly target: ReplyTarget
      readonly limit: number
    }): Effect.Effect<ReadonlyArray<ReadMessage>, ChannelError>
    /**
     * Asks the owner, in a DM, whether to start proposed work, with Start and Skip buttons. A click arrives as an
     * `ApprovalDecision` event for `proposalId`. Returns the DM message.
     */
    requestApproval(input: {
      readonly ownerUserId: string
      readonly proposalId: string
      readonly thread: ThreadRef
      readonly repo: string
      readonly title: string
      readonly why: string
      /** Shown in full: the owner approves exactly what the worker will run. */
      readonly brief: string
      readonly operationId: OperationId
    }): Effect.Effect<MessageRef, ChannelError>
    /** Replaces the buttons with the outcome, so a decided proposal cannot be clicked again. */
    closeApproval(input: {
      readonly message: MessageRef
      readonly text: string
    }): Effect.Effect<void, ChannelError>
    /**
     * Posts a worker's request in its thread: the text as Markdown, its images under it, then Approve and Change
     * buttons. Approve arrives as a `WorkerAnswer` event for `askId`; Change first asks what to change, in a form.
     * Returns the message and the uploaded files, so the request can be shown again without its buttons.
     */
    askApproval(input: {
      readonly thread: ThreadRef
      readonly askId: string
      readonly text: string
      /** Paths of image files. */
      readonly images: ReadonlyArray<string>
      readonly operationId: OperationId
    }): Effect.Effect<AskMessage, ChannelError>
    /** Shows a request again with the outcome in place of its buttons, so it cannot be answered twice. */
    closeAsk(input: {
      readonly ask: AskMessage
      readonly text: string
      readonly outcome: string
    }): Effect.Effect<void, ChannelError>
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
