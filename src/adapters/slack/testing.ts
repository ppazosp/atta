import type {
  ChatPostMessageArguments,
  ChatUpdateArguments,
  ConversationsHistoryArguments,
  ConversationsRepliesArguments,
  ReactionsAddArguments,
  ReactionsRemoveArguments,
} from "@slack/web-api"
import type { AppOptions } from "@slack/bolt"
import { Effect } from "effect"
import { SlackChannelLayer } from "./index.ts"
import type { SlackClient } from "./api.ts"

const config = { botToken: "test-bot-token", appToken: "test-app-token", botUserId: "UBOT" }
const ignoreError = async () => undefined
export const thread = { team: "T01", conversation: "D01", rootTs: "1700000000.000100" }
export const plan = { team: "T01", conversation: "D01", ts: "1700000001.000100" }
export const envelope = (event: unknown, eventId = "Ev01") => ({
  type: "event_callback",
  event_id: eventId,
  team_id: "T01",
  event,
})
export const message = {
  type: "message",
  user: "U01",
  channel: "D01",
  channel_type: "im",
  ts: thread.rootTs,
  text: "hello",
}

export const makeFakeSlack = () => {
  const handlers = new Map<string, (args: { body: unknown }) => Promise<void>>()
  const calls = {
    post: [] as Array<ChatPostMessageArguments>,
    update: [] as Array<ChatUpdateArguments>,
    replies: [] as Array<ConversationsRepliesArguments>,
    history: [] as Array<ConversationsHistoryArguments>,
    add: [] as Array<ReactionsAddArguments>,
    remove: [] as Array<ReactionsRemoveArguments>,
  }
  const responses = {
    post: async (): Promise<unknown> => ({ ok: true, ts: plan.ts }),
    update: async (): Promise<unknown> => ({ ok: true }),
    replies: async (): Promise<unknown> => ({ ok: true, messages: [] }),
    history: async (): Promise<unknown> => ({ ok: true, messages: [] }),
    add: async (): Promise<unknown> => ({ ok: true }),
    remove: async (): Promise<unknown> => ({ ok: true }),
    start: async (): Promise<unknown> => undefined,
  }
  const lifecycle = { started: 0, stopped: 0, options: undefined as AppOptions | undefined }
  let onError: (error: unknown) => Promise<void> = ignoreError
  const client: SlackClient = {
    chat: {
      postMessage: (input) => {
        calls.post.push(input)
        return responses.post()
      },
      update: (input) => {
        calls.update.push(input)
        return responses.update()
      },
    },
    conversations: {
      replies: (input) => {
        calls.replies.push(input)
        return responses.replies()
      },
      history: (input) => {
        calls.history.push(input)
        return responses.history()
      },
    },
    reactions: {
      add: (input) => {
        calls.add.push(input)
        return responses.add()
      },
      remove: (input) => {
        calls.remove.push(input)
        return responses.remove()
      },
    },
  }
  const layer = SlackChannelLayer(config, {
    client,
    makeApp: (options) => {
      lifecycle.options = options
      return {
        event: (name, handler) => {
          handlers.set(name, handler)
        },
        error: (handler) => {
          onError = handler
        },
        start: async () => {
          lifecycle.started++
          return responses.start()
        },
        stop: async () => {
          lifecycle.stopped++
        },
      }
    },
  })
  return {
    calls,
    responses,
    lifecycle,
    layer,
    emit: (name: string, body: unknown) => Effect.promise(() => handlers.get(name)!({ body })),
    error: (cause: unknown) => Effect.promise(() => onError(cause)),
  }
}
