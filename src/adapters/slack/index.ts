import { App } from "@slack/bolt"
import type { AppOptions } from "@slack/bolt"
import { WebClient } from "@slack/web-api"
import { Cause, Clock, Effect, Layer, Option, Queue, Schema, Stream } from "effect"
import type { ReplyTarget } from "../../ports/Channel.ts"
import { Channel, ChannelError } from "../../ports/Channel.ts"
import { call, channelError, escapeMrkdwn, metadata, Ok, Posted } from "./api.ts"
import type { SlackClient } from "./api.ts"
import { decodeEvent } from "./events.ts"
import { makePlanWriter, planContent } from "./plan.ts"

interface SlackApp {
  event(name: "message" | "app_mention", handler: (args: { body: unknown }) => Promise<void>): void
  error(handler: (error: unknown) => Promise<void>): void
  start(): Promise<unknown>
  stop(): Promise<unknown>
}

const Config = Schema.Struct({
  botToken: Schema.NonEmptyString,
  appToken: Schema.NonEmptyString,
  botUserId: Schema.NonEmptyString,
})
const Messages = Schema.Struct({
  messages: Schema.Array(
    Schema.Struct({
      ts: Schema.NonEmptyString,
      user: Schema.optionalKey(Schema.String),
      metadata: Schema.optionalKey(Schema.Unknown),
    }),
  ),
  response_metadata: Schema.optionalKey(Schema.Struct({ next_cursor: Schema.optionalKey(Schema.String) })),
})
const OperationMetadata = Schema.Struct({
  event_type: Schema.Literal("atta_operation"),
  event_payload: Schema.Struct({ operation_id: Schema.NonEmptyString }),
})
const targetArgs = (target: ReplyTarget) =>
  "thread" in target
    ? { channel: target.thread.conversation, thread_ts: target.thread.rootTs }
    : { channel: target.conversation.conversation }

export const SlackChannelLayer = (
  config: typeof Config.Type,
  dependencies?: { readonly client: SlackClient; readonly makeApp: (options: AppOptions) => SlackApp },
) =>
  Layer.effect(
    Channel,
    Effect.gen(function* () {
      const options = yield* Schema.decodeUnknownEffect(Config)(config).pipe(
        Effect.mapError(
          () => new ChannelError({ operation: "config", message: "Invalid Slack configuration" }),
        ),
      )
      const inbox = yield* Queue.make<
        { readonly payload: unknown; readonly receivedAt: number },
        ChannelError
      >()
      const clock = yield* Clock.Clock
      const scope = yield* Effect.scope
      const app = yield* Effect.try({
        try: () => {
          const appOptions: AppOptions = {
            appToken: options.appToken,
            socketMode: true,
            authorize: async () => ({ botToken: options.botToken, botUserId: options.botUserId }),
            ignoreSelf: false,
          }
          return dependencies ? dependencies.makeApp(appOptions) : new App(appOptions)
        },
        catch: (cause) => channelError("start", cause),
      })
      const client =
        dependencies?.client ??
        // On a rate limit, wait as long as Slack says and try again, instead of failing a reply or reaction.
        new WebClient(options.botToken, { retryConfig: { retries: 2 } })
      const receive = async ({ body }: { body: unknown }) => {
        Queue.offerUnsafe(inbox, { payload: body, receivedAt: clock.currentTimeMillisUnsafe() })
      }
      app.event("message", receive)
      app.event("app_mention", receive)
      app.error(async (cause) => {
        Queue.failCauseUnsafe(inbox, Cause.fail(channelError("events", cause)))
      })
      yield* Effect.addFinalizer(() =>
        Effect.tryPromise({ try: () => app.stop(), catch: (cause) => channelError("stop", cause) }).pipe(
          Effect.catch((error) => Effect.logError(error)),
        ),
      )
      yield* Effect.tryPromise({ try: () => app.start(), catch: (cause) => channelError("start", cause) })
      const plans = makePlanWriter(client, scope)

      return Channel.of({
        events: Stream.fromQueue(inbox).pipe(
          // A payload atta cannot read is logged and skipped; it must not stop every later event.
          Stream.mapEffect(({ payload, receivedAt }) =>
            decodeEvent(payload, options.botUserId, receivedAt).pipe(
              Effect.catch((error) =>
                Effect.logWarning("skipped a Slack event atta could not read", error).pipe(
                  Effect.as(Option.none()),
                ),
              ),
            ),
          ),
          Stream.filter(Option.isSome),
          Stream.map((event) => event.value),
        ),
        reply: Effect.fn("Slack.reply")(function* ({ target, text, operationId }) {
          // oxlint-disable unicorn/require-post-message-target-origin -- This is Slack's Web API.
          const result = yield* call(
            "chat.postMessage",
            () =>
              client.chat.postMessage({
                ...targetArgs(target),
                text: escapeMrkdwn(text),
                metadata: metadata(operationId),
              }),
            Posted,
          )
          // oxlint-enable unicorn/require-post-message-target-origin
          const where = "thread" in target ? target.thread : target.conversation
          return { team: where.team, conversation: where.conversation, ts: result.ts }
        }),
        findReply: Effect.fn("Slack.findReply")(function* ({ target, operationId }) {
          const where = "thread" in target ? target.thread : target.conversation
          let cursor: string | undefined
          do {
            const args = {
              channel: where.conversation,
              include_all_metadata: true,
              limit: 100,
              ...(cursor ? { cursor } : {}),
            }
            const result =
              "thread" in target
                ? yield* call(
                    "conversations.replies",
                    () => client.conversations.replies({ ...args, ts: target.thread.rootTs }),
                    Messages,
                  )
                : yield* call("conversations.history", () => client.conversations.history(args), Messages)
            for (const message of result.messages) {
              const tag = Schema.decodeUnknownOption(OperationMetadata)(message.metadata)
              // Only atta's own messages count: anyone could post a message with the same metadata.
              if (
                message.user === options.botUserId &&
                Option.isSome(tag) &&
                tag.value.event_payload.operation_id === operationId
              ) {
                return Option.some({ team: where.team, conversation: where.conversation, ts: message.ts })
              }
            }
            cursor = "thread" in target ? result.response_metadata?.next_cursor?.trim() : undefined
          } while (cursor)
          return Option.none()
        }),
        react: Effect.fn("Slack.react")((message, emoji) =>
          call(
            "reactions.add",
            () => client.reactions.add({ channel: message.conversation, timestamp: message.ts, name: emoji }),
            Ok,
          ).pipe(
            Effect.asVoid,
            Effect.catch((error) => (error.message === "already_reacted" ? Effect.void : Effect.fail(error))),
          ),
        ),
        unreact: Effect.fn("Slack.unreact")((message, emoji) =>
          call(
            "reactions.remove",
            () =>
              client.reactions.remove({ channel: message.conversation, timestamp: message.ts, name: emoji }),
            Ok,
          ).pipe(
            Effect.asVoid,
            Effect.catch((error) => (error.message === "no_reaction" ? Effect.void : Effect.fail(error))),
          ),
        ),
        upsertPlan: Effect.fn("Slack.upsertPlan")(function* ({ thread, plan, title, tasks, operationId }) {
          const content = yield* planContent(title, tasks)
          if (plan) {
            yield* plans.edit(plan, content)
            return plan
          }
          // oxlint-disable unicorn/require-post-message-target-origin -- This is Slack's Web API.
          const result = yield* call(
            "chat.postMessage",
            () =>
              client.chat.postMessage({
                channel: thread.conversation,
                thread_ts: thread.rootTs,
                ...content,
                metadata: metadata(operationId),
              }),
            Posted,
          )
          // oxlint-enable unicorn/require-post-message-target-origin
          const message = { team: thread.team, conversation: thread.conversation, ts: result.ts }
          yield* plans.posted(message)
          return message
        }),
        finishPlan: Effect.fn("Slack.finishPlan")(function* ({ plan, title, tasks, answer }) {
          // The task icons tell how the run ended (core marks unfinished steps), so the title stays as it is.
          const content = yield* planContent(title, tasks, answer)
          yield* plans.edit(plan, content, true)
        }),
      })
    }),
  )
