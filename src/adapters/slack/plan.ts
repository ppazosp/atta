import type { ChatUpdateArguments } from "@slack/web-api"
import { Clock, Deferred, Effect, Scope } from "effect"
import type { MessageRef } from "../../core/domain/thread.ts"
import type { PlanTask } from "../../ports/Channel.ts"
import { ChannelError } from "../../ports/Channel.ts"
import { call, Ok, escapeMrkdwn } from "./api.ts"
import type { SlackClient } from "./api.ts"

type Blocks = Extract<ChatUpdateArguments, { blocks: unknown }>["blocks"]

export const planContent = Effect.fn("Slack.planContent")(function* (
  title: string,
  tasks: ReadonlyArray<PlanTask>,
  answer: string | null = null,
) {
  let visible = [...tasks]
  // A message holds at most 50 blocks: the title, the answer and up to 48 cards.
  if (tasks.length > 48) {
    const count = tasks.length - 47
    const finished = tasks
      .filter((task) => task.status === "complete" || task.status === "error")
      .slice(0, count)
    if (finished.length < count) {
      return yield* new ChannelError({
        operation: "plan",
        message: "More than 47 unfinished tasks cannot fit alongside a summary task",
      })
    }
    const grouped = new Set(finished)
    const errors = finished.filter((task) => task.status === "error").length
    const usedIds = new Set(tasks.map((task) => task.id))
    let summaryId = "atta_finished_summary"
    while (usedIds.has(summaryId)) summaryId += "_"
    visible = [
      {
        id: summaryId,
        title: `${finished.length} earlier tasks finished${errors ? ` (${errors} failed)` : ""}`,
        status: errors ? "error" : "complete",
        detail: null,
      },
      ...tasks.filter((task) => !grouped.has(task)),
    ]
  }
  // Slack's timeline style: a title line, then one task card per step of the plan. A card shows its own spinner,
  // ✓ or ⚠️ (a step inside a plan block shows only a dot when done). Slack rejects a pending card, so a step the
  // worker has not started yet spins too: the whole plan stays visible, and each step gets its ✓ as it finishes.
  const blocks: Blocks = [
    { type: "section", text: { type: "mrkdwn", text: `*${escapeMrkdwn(title)}*` } },
    ...visible.map((task) => ({
      type: "task_card" as const,
      task_id: task.id,
      title: task.title,
      status: task.status === "pending" ? ("in_progress" as const) : task.status,
      ...(task.detail === null
        ? {}
        : {
            details: {
              type: "rich_text" as const,
              elements: [
                {
                  type: "rich_text_section" as const,
                  elements: [{ type: "text" as const, text: task.detail }],
                },
              ],
            },
          }),
    })),
  ]
  // Rich text is never parsed as markup, so a model's answer cannot hide a link or ping anyone.
  if (answer) {
    blocks.push({
      type: "rich_text",
      elements: [{ type: "rich_text_section", elements: [{ type: "text", text: answer }] }],
    })
  }
  const fallback = answer ? `${title}\n\n${answer}` : title
  return { text: escapeMrkdwn(fallback), blocks }
})

type Content = Effect.Success<ReturnType<typeof planContent>>
type Pending = {
  content: Content
  readonly waiters: Array<Deferred.Deferred<void, ChannelError>>
}
type Writer = {
  lastWrite: number
  running: boolean
  terminal: boolean
  pending: Pending | undefined
}
const key = (message: MessageRef) => JSON.stringify([message.team, message.conversation, message.ts])

export const makePlanWriter = (client: SlackClient, scope: Scope.Scope) => {
  const writers = new Map<string, Writer>()
  const get = (message: MessageRef): Writer => {
    const id = key(message)
    let state = writers.get(id)
    if (!state) {
      state = { lastWrite: -Infinity, running: false, terminal: false, pending: undefined }
      writers.set(id, state)
    }
    return state
  }

  const drain = Effect.fn("Slack.drainPlan")(function* (message: MessageRef, state: Writer) {
    while (state.pending) {
      const now = yield* Clock.currentTimeMillis
      yield* Effect.sleep(Math.max(0, state.lastWrite + 1_000 - now))
      const pending = state.pending
      if (!pending) break
      state.pending = undefined
      state.lastWrite = yield* Clock.currentTimeMillis
      const result = yield* call(
        "chat.update",
        () => client.chat.update({ channel: message.conversation, ts: message.ts, ...pending.content }),
        Ok,
      ).pipe(Effect.asVoid, Effect.exit)
      for (const waiter of pending.waiters) yield* Deferred.done(waiter, result)
    }
    state.running = false
  })

  return {
    posted: Effect.fn("Slack.planPosted")(function* (message: MessageRef) {
      get(message).lastWrite = yield* Clock.currentTimeMillis
    }),
    edit: Effect.fn("Slack.editPlan")(function* (message: MessageRef, content: Content, terminal = false) {
      const state = get(message)
      if (state.terminal && !terminal) return
      const waiter = yield* Deferred.make<void, ChannelError>()
      if (state.pending) {
        state.pending.content = content
        state.pending.waiters.push(waiter)
      } else {
        state.pending = { content, waiters: [waiter] }
      }
      if (terminal) state.terminal = true
      if (!state.running) {
        state.running = true
        yield* Effect.forkIn(drain(message, state), scope)
      }
      yield* Deferred.await(waiter)
    }),
  }
}
