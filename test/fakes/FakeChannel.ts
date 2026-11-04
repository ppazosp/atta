import { Context, Effect, Layer, Option, Queue, Ref, Stream } from "effect"
import type { Event } from "../../src/core/domain/event.ts"
import type { MessageRef } from "../../src/core/domain/thread.ts"
import type { AskMessage, PlanTask, ReadMessage, ReplyTarget } from "../../src/ports/Channel.ts"
import { Channel } from "../../src/ports/Channel.ts"

type UserMessage = Extract<Event, { _tag: "UserMessage" | "ApprovalDecision" | "WorkerAnswer" }>

export type ChannelCall =
  | {
      readonly _tag: "reply"
      readonly target: ReplyTarget
      readonly text: string
      readonly operationId: string
      readonly message: MessageRef
    }
  | { readonly _tag: "react"; readonly message: MessageRef; readonly emoji: string }
  | { readonly _tag: "unreact"; readonly message: MessageRef; readonly emoji: string }
  | {
      readonly _tag: "upsertPlan"
      readonly operationId: string
      readonly plan: MessageRef
      readonly title: string
      readonly tasks: ReadonlyArray<PlanTask>
    }
  | {
      readonly _tag: "requestApproval"
      readonly proposalId: string
      readonly title: string
      readonly repo: string
      readonly why: string
      readonly brief: string
      readonly message: MessageRef
    }
  | { readonly _tag: "closeApproval"; readonly message: MessageRef; readonly text: string }
  | {
      readonly _tag: "askApproval"
      readonly askId: string
      readonly text: string
      readonly images: ReadonlyArray<string>
      readonly operationId: string
      readonly message: MessageRef
    }
  | {
      readonly _tag: "closeAsk"
      readonly ask: AskMessage
      readonly text: string
      readonly outcome: string
    }
  | {
      readonly _tag: "finishPlan"
      readonly plan: MessageRef
      readonly outcome: string
      readonly answer: string | null
    }

/** Lets a test push Slack messages in and read what atta sent out. */
export class FakeChannelControl extends Context.Service<
  FakeChannelControl,
  {
    push(event: UserMessage): Effect.Effect<void>
    readonly calls: Effect.Effect<ReadonlyArray<ChannelCall>>
  }
>()("atta/test/FakeChannelControl") {}

export const FakeChannelLayer = Layer.effectContext(
  Effect.gen(function* () {
    const inbox = yield* Queue.unbounded<UserMessage>()
    const calls = yield* Ref.make<ReadonlyArray<ChannelCall>>([])
    // Every pushed message, as Slack would keep it in the conversation's history.
    const history = yield* Ref.make<ReadonlyArray<Extract<UserMessage, { _tag: "UserMessage" }>>>([])
    const record = (call: ChannelCall) => Ref.update(calls, (all) => [...all, call])
    let ts = 1_700_000_000
    const nextMessage = (team: string, conversation: string): MessageRef => ({
      team,
      conversation,
      ts: `${ts++}.000100`,
    })

    const channel = Channel.of({
      events: Stream.fromQueue(inbox),
      reply: ({ target, text, operationId }) =>
        Effect.gen(function* () {
          const where = "thread" in target ? target.thread : target.conversation
          const message = nextMessage(where.team, where.conversation)
          yield* record({ _tag: "reply", target, text, operationId, message })
          return message
        }),
      // Like Slack, both replies and plan messages carry their operation ID.
      findReply: ({ operationId }) =>
        Ref.get(calls).pipe(
          Effect.map((all) => {
            for (const c of all) {
              if (c._tag === "reply" && c.operationId === operationId) return Option.some(c.message)
              if (c._tag === "upsertPlan" && c.operationId === operationId) return Option.some(c.plan)
              if (c._tag === "askApproval" && c.operationId === operationId) return Option.some(c.message)
            }
            return Option.none()
          }),
        ),
      readConversation: ({ target, limit }) =>
        Ref.get(history).pipe(
          Effect.map((all): ReadonlyArray<ReadMessage> => {
            const inTarget = all.filter((m) =>
              "thread" in target
                ? m.thread.conversation === target.thread.conversation &&
                  m.thread.rootTs === target.thread.rootTs
                : m.thread.conversation === target.conversation.conversation,
            )
            return inTarget.slice(-limit).map((m) => ({ userId: m.userId, ts: m.message.ts, text: m.text }))
          }),
        ),
      requestApproval: ({ ownerUserId, proposalId, title, repo, why, brief }) =>
        Effect.gen(function* () {
          // The owner's DM, as Slack would open it.
          const message = nextMessage("T01", `D-${ownerUserId}`)
          yield* record({ _tag: "requestApproval", proposalId, title, repo, why, brief, message })
          return message
        }),
      closeApproval: ({ message, text }) => record({ _tag: "closeApproval", message, text }),
      askApproval: ({ thread, askId, text, images, operationId }) =>
        Effect.gen(function* () {
          const message = nextMessage(thread.team, thread.conversation)
          yield* record({ _tag: "askApproval", askId, text, images, operationId, message })
          // One uploaded file per image, as Slack would name it.
          return { message, files: images.map((_, index) => `F${index}`) }
        }),
      // Slack's limit, without the adapter's escaping.
      askFits: (text) => text.length <= 12_000,
      closeAsk: ({ ask, text, outcome }) => record({ _tag: "closeAsk", ask, text, outcome }),
      react: (message, emoji) => record({ _tag: "react", message, emoji }),
      unreact: (message, emoji) => record({ _tag: "unreact", message, emoji }),
      upsertPlan: ({ thread, plan, title, tasks, operationId }) =>
        Effect.gen(function* () {
          const message = plan ?? nextMessage(thread.team, thread.conversation)
          yield* record({ _tag: "upsertPlan", operationId, plan: message, title, tasks })
          return message
        }),
      finishPlan: ({ plan, outcome, answer }) => record({ _tag: "finishPlan", plan, outcome, answer }),
    })
    const control = FakeChannelControl.of({
      push: (event) =>
        (event._tag === "UserMessage" ? Ref.update(history, (all) => [...all, event]) : Effect.void).pipe(
          Effect.andThen(Queue.offer(inbox, event)),
          Effect.asVoid,
        ),
      calls: Ref.get(calls),
    })
    return Context.make(Channel, channel).pipe(Context.add(FakeChannelControl, control))
  }),
)
