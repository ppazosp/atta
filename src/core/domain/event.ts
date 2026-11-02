import { Schema } from "effect"
import { EventId, RunId, Timestamp, WorkerId } from "./ids.ts"
import { ScheduleTarget } from "./config.ts"
import { MessageRef, ThreadRef } from "./thread.ts"
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
    /** `channel`: a message in a watched channel that does not mention atta, from anyone. */
    via: Schema.Literals(["dm", "mention", "thread_reply", "channel"]),
  },
  /** A prompt from the config file, due at `slot`, for an existing thread or a new one. */
  ScheduledPrompt: {
    id: EventId,
    source: Schema.Literal("clock"),
    externalId: Schema.NonEmptyString,
    receivedAt: Timestamp,
    schedule: Schema.NonEmptyString,
    slot: Timestamp,
    target: ScheduleTarget,
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
  /**
   * The router proposed work on a message it observed in a watched channel. Stored so the owner's decision can
   * find it; it is never routed.
   */
  WorkProposed: {
    id: EventId,
    source: Schema.Literal("atta"),
    externalId: Schema.NonEmptyString,
    receivedAt: Timestamp,
    /** The channel thread the work is about, where it runs if the owner approves. */
    thread: ThreadRef,
    repo: Schema.NonEmptyString,
    title: Schema.NonEmptyString,
    brief: Schema.NonEmptyString,
    why: Schema.NonEmptyString,
  },
  /** Someone clicked Start or Skip on a proposal. Only the trusted user's first click counts. */
  ApprovalDecision: {
    id: EventId,
    source: Schema.Literal("slack"),
    /** `decision:<proposal ID>`, so only the first decision on a proposal is stored. */
    externalId: Schema.NonEmptyString,
    receivedAt: Timestamp,
    proposalId: EventId,
    decision: Schema.Literals(["approve", "reject"]),
    userId: Schema.NonEmptyString,
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
