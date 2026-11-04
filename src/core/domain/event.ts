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
  /** A message from someone in Slack: a DM, a mention, or any message in a channel atta is in. */
  UserMessage: {
    id: EventId,
    source: Schema.Literal("slack"),
    externalId: Schema.NonEmptyString,
    receivedAt: Timestamp,
    thread: ThreadRef,
    message: MessageRef,
    userId: Schema.NonEmptyString,
    text: Schema.String,
    /** `channel`: a top-level message that does not mention atta, from anyone, in a channel atta is in. */
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
   * The router proposed a worker (`start_worker`): every worker waits for the owner's approval. Stored so the
   * decision can find it; it is never routed.
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
    /**
     * `decision:<proposal ID>:<user ID>`, so only a user's first click on a proposal is stored, and someone else's
     * click cannot take the owner's place.
     */
    externalId: Schema.NonEmptyString,
    receivedAt: Timestamp,
    proposalId: EventId,
    decision: Schema.Literals(["approve", "reject"]),
    userId: Schema.NonEmptyString,
  },
  /**
   * A worker asked the user to approve what it plans to do (`ask_approval`), with Approve and Change buttons.
   * Stored so the user's answer can find it; it is never routed.
   */
  WorkerAsked: {
    id: EventId,
    source: Schema.Literal("worker"),
    externalId: Schema.NonEmptyString,
    receivedAt: Timestamp,
    workerId: WorkerId,
    text: Schema.NonEmptyString,
    /** Image files in the worker's worktree, shown under the text. */
    images: Schema.Array(Schema.NonEmptyString),
  },
  /** Someone answered a worker's request: Approve, or Change with what to change. Only the trusted user counts. */
  WorkerAnswer: {
    id: EventId,
    source: Schema.Literal("slack"),
    /** `answer:<request ID>:<user ID>`, so only a user's first answer to a request is stored. */
    externalId: Schema.NonEmptyString,
    receivedAt: Timestamp,
    askId: EventId,
    decision: Schema.Literals(["approve", "change"]),
    /** What to change; null for an approval. */
    text: Schema.NullOr(Schema.String),
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
