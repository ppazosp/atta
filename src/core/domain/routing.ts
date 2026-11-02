import { Schema } from "effect"
import type { Event } from "./event.ts"
import { WorkerId } from "./ids.ts"
import type { WorkerState } from "./worker.ts"

/** Where one event goes. Dispatch (WP5) carries it out; this module only decides. */
export const Route = Schema.TaggedUnion({
  Drop: { reason: Schema.Literals(["untrusted_sender", "unknown_thread", "not_for_atta"]) },
  ToRouter: {},
  /** A message in a watched channel: the router may propose work, but nothing is posted and no reactions. */
  Observe: {},
  /** The trusted user decided on a proposal: core starts the work or drops it, without the router. */
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
 * The routing rules from docs/v0-plan.md, "Flow of one message":
 * - only Slack messages are filtered by sender; events atta creates itself are trusted by kind
 * - a thread reply counts only in a thread atta is already part of
 * - "stop" stops the thread's running workers without the router
 * - other messages go to the thread's worker when it is the only subscriber and is running or idle; otherwise to
 *   the router
 * - scheduled prompts, worker reports and finished runs always go to the router
 */
export const route = (event: Event, context: RoutingContext): Route => {
  switch (event._tag) {
    case "UserMessage": {
      const trusted = event.userId === context.trustedUserId
      // Someone else's message only matters in a watched channel, and only as something to observe.
      if (!trusted) {
        return context.watched
          ? Route.cases.Observe.make({})
          : Route.cases.Drop.make({ reason: "untrusted_sender" })
      }
      if (event.via === "channel") {
        return context.watched
          ? Route.cases.Observe.make({})
          : Route.cases.Drop.make({ reason: "not_for_atta" })
      }
      if (event.via === "thread_reply" && !context.threadKnown) {
        return context.watched
          ? Route.cases.Observe.make({})
          : Route.cases.Drop.make({ reason: "unknown_thread" })
      }
      const running = context.subscribers.filter((s) => s.state === "running")
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
