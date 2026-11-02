import type { Event } from "./domain/event.ts"
import { threadKey, type ThreadKey } from "./domain/thread.ts"
import type { Worker } from "./domain/worker.ts"

/** The conversation key for a new thread in a conversation: `slack:<team>/<conversation>`. */
export const conversationKey = (c: { readonly team: string; readonly conversation: string }) =>
  `slack:${c.team}/${c.conversation}`

/** How an event reads to the router. Every event names the thread or conversation `reply` should answer in. */
export const renderEvent = (
  event: Event,
  worker: Worker | null,
  openedThread: ThreadKey | null = null,
): string => {
  switch (event._tag) {
    case "UserMessage":
      return [
        `[event ${event.id}] Slack message from the user (${event.via}) in thread ${threadKey(event.thread)}:`,
        event.text,
      ].join("\n")
    case "ScheduledPrompt": {
      const where =
        openedThread !== null
          ? `thread ${openedThread}`
          : "thread" in event.target
            ? `thread ${threadKey(event.target.thread)}`
            : `a new thread in ${conversationKey(event.target.conversation)}`
      return [
        `[event ${event.id}] Scheduled prompt "${event.schedule}", answer in ${where}:`,
        event.text,
      ].join("\n")
    }
    case "WorkerReport":
      return [
        `[event ${event.id}] Worker ${event.workerId}${describe(worker)} reports:`,
        quoteWorker(event.text),
      ].join("\n")
    case "WorkerFinished":
      return [
        `[event ${event.id}] Worker ${event.workerId}${describe(worker)} finished a run: ${event.outcome}.`,
        `Branch: ${event.branch ?? "unknown"}. Commit: ${event.commit ?? "none"}.`,
        ...(event.wrongBranch
          ? ["Warning: it committed outside the branch created for it. Tell the user."]
          : []),
        "Its final message:",
        event.finalMessage === null ? "(none)" : quoteWorker(event.finalMessage),
      ].join("\n")
  }
}

/**
 * Wraps text a worker wrote, which may quote files or tool output, so it cannot pass for an event or for the
 * user. The marker is random per quote, so the text cannot close the block early.
 */
const quoteWorker = (text: string) => {
  const marker = crypto.randomUUID().slice(0, 8)
  return [`<<<worker-text ${marker}`, text, `worker-text ${marker}>>>`].join("\n")
}

const describe = (worker: Worker | null) =>
  worker ? ` ("${worker.brief.title}" on ${worker.brief.repo}, from thread ${worker.origin})` : ""
