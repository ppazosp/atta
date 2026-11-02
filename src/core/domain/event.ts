import { Schema } from "effect"
import { EventId, RunId, Timestamp, WorkerId } from "./ids.ts"
import { Conversation, MessageRef, ThreadRef } from "./thread.ts"
import { RunOutcome } from "./worker.ts"

/**
 * Everything atta reacts to. Events are stored once and never edited.
 *
 * `externalId` is unique per `source` and makes storing an event idempotent: a Slack retry or a clock slot that
 * fires twice is stored once.
 */
export const Event = Schema.TaggedUnion({
  /** A message from someone in Slack: a DM, a mention, or a reply in a thread atta is part of. */
  UserMessage: {
    id: EventId,
    source: Schema.Literal("slack"),
    externalId: Schema.NonEmptyString,
    receivedAt: Timestamp,
    thread: ThreadRef,
    message: MessageRef,
    userId: Schema.NonEmptyString,
    text: Schema.String,
    via: Schema.Literals(["dm", "mention", "thread_reply"]),
  },
  /** A prompt from the config file, due at `slot`. It starts a new thread in `conversation`. */
  ScheduledPrompt: {
    id: EventId,
    source: Schema.Literal("clock"),
    externalId: Schema.NonEmptyString,
    receivedAt: Timestamp,
    schedule: Schema.NonEmptyString,
    slot: Timestamp,
    conversation: Conversation,
    text: Schema.NonEmptyString,
  },
  /** A worker called `report` on atta's MCP server. */
  WorkerReport: {
    id: EventId,
    source: Schema.Literal("worker"),
    externalId: Schema.NonEmptyString,
    receivedAt: Timestamp,
    workerId: WorkerId,
    text: Schema.String,
  },
  /** The tracker saw a worker run end. */
  WorkerFinished: {
    id: EventId,
    source: Schema.Literal("worker"),
    externalId: Schema.NonEmptyString,
    receivedAt: Timestamp,
    workerId: WorkerId,
    runId: RunId,
    outcome: RunOutcome,
    finalMessage: Schema.NullOr(Schema.String),
    branch: Schema.NullOr(Schema.String),
    commit: Schema.NullOr(Schema.String),
    /** True when the worker committed somewhere other than the branch the runtime created for it. */
    wrongBranch: Schema.Boolean,
  },
})
export type Event = typeof Event.Type
export type EventSource = Event["source"]
