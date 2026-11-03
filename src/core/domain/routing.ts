import { Schema } from "effect"
import type { Event } from "./event.ts"
import { WorkerId } from "./ids.ts"
import type { WorkerState } from "./worker.ts"

/** Where one event goes. Dispatch (WP5) carries it out; this module only decides. */
export const Route = Schema.TaggedUnion({
  Drop: { reason: Schema.Literals(["untrusted_sender", "unknown_thread", "not_for_atta"]) },
  ToRouter: {},
  /**
   * A message in a watched channel that is information, not a request: from anyone, or from the user without a
   * mention. It waits for the next channel turn, where the router can only propose work.
   */
  Observe: {},
  /** The trusted user decided on a proposal: core starts the work or drops it, then tells the router. */
  Decide: {},
  /** Stored for reference only (a proposal). */
  Record: {},
  /** Straight to the thread's worker, without the router: a steer while it runs, a new run while it is idle. */
  ToWorker: { workerId: WorkerId, mode: Schema.Literals(["steer", "queue"]) },
  StopWorkers: { workerIds: Schema.Array(WorkerId) },
})
export type Route = typeof Route.Type

export interface Subscriber {
  readonly workerId: WorkerId
  readonly state: WorkerState
}

export interface RoutingContext {
  /** v0 trusts exactly one Slack user. */
  readonly trustedUserId: string
  /** Workers subscribed to the event's thread. */
  readonly subscribers: ReadonlyArray<Subscriber>
  /**
   * Whether atta already knew the event's thread before this event. Slack sends every reply in channels atta
   * is in, so a thread reply only counts when the thread is one atta is part of.
   */
  readonly threadKnown: boolean
  /** Whether the event's conversation is one of the watched channels. */
  readonly watched: boolean
}

export const isStopCommand = (text: string) =>
  text
    .trim()
    .toLowerCase()
    .replace(/[.!]+$/, "") === "stop"

/**
 * The routing rules from docs/v0-plan.md, "Flow of one message", and docs/watched-channels.md:
 * - only Slack messages are filtered by sender; events atta creates itself are trusted by kind
 * - the user talks to atta in a DM or with a mention. In a channel, nothing else is a request
 * - in a thread atta is part of, only the user's mentions (and "stop") reach atta; anything else waits until the
 *   user asks atta to read the thread
 * - elsewhere in a watched channel, every other message is information for the next channel turn
 * - "stop" stops the thread's running workers without the router
 * - a request goes to the thread's worker when it is the only subscriber and is running or idle; otherwise to the
 *   router
 * - scheduled prompts, worker reports and finished runs always go to the router
 */
export const route = (event: Event, context: RoutingContext): Route => {
  switch (event._tag) {
    case "UserMessage": {
      const trusted = event.userId === context.trustedUserId
      const running = context.subscribers.filter((s) => s.state === "running")
      const request = trusted && (event.via === "dm" || event.via === "mention")
      if (!request) {
        // "stop" needs no mention: stopping is always safe, and it should work however the user types it.
        if (trusted && context.threadKnown && running.length > 0 && isStopCommand(event.text)) {
          return Route.cases.StopWorkers.make({ workerIds: running.map((s) => s.workerId) })
        }
        if (context.threadKnown) {
          return Route.cases.Drop.make({ reason: trusted ? "not_for_atta" : "untrusted_sender" })
        }
        if (context.watched) return Route.cases.Observe.make({})
        return Route.cases.Drop.make({
          reason: !trusted
            ? "untrusted_sender"
            : event.via === "thread_reply"
              ? "unknown_thread"
              : "not_for_atta",
        })
      }
      if (isStopCommand(event.text)) {
        return running.length > 0
          ? Route.cases.StopWorkers.make({ workerIds: running.map((s) => s.workerId) })
          : Route.cases.ToRouter.make({})
      }
      // Pass-through only when the thread has exactly one subscriber and it can take a message.
      const [only, ...others] = context.subscribers
      if (only === undefined || others.length > 0 || (only.state !== "running" && only.state !== "idle")) {
        return Route.cases.ToRouter.make({})
      }
      return Route.cases.ToWorker.make({
        workerId: only.workerId,
        mode: only.state === "running" ? "steer" : "queue",
      })
    }
    case "ApprovalDecision":
      return event.userId === context.trustedUserId
        ? Route.cases.Decide.make({})
        : Route.cases.Drop.make({ reason: "untrusted_sender" })
    case "WorkProposed":
      return Route.cases.Record.make({})
    case "ScheduledPrompt":
    case "WorkerReport":
    case "WorkerFinished":
      return Route.cases.ToRouter.make({})
  }
}
