import type {
  ChatPostMessageArguments,
  ChatGetPermalinkArguments,
  ChatUpdateArguments,
  ConversationsHistoryArguments,
  ConversationsOpenArguments,
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
export const buttonAction = (actionId: string, value = "proposal01", actionTs = "1700000002.000100") => ({
  type: "block_actions",
  user: { id: "U01" },
  actions: [{ type: "button", action_id: actionId, value, action_ts: actionTs }],
})

export const makeFakeSlack = () => {
  const handlers = new Map<string, (args: { body: unknown }) => Promise<void>>()
  const actions = new Map<string, (args: { body: unknown; ack: () => Promise<unknown> }) => Promise<void>>()
  const calls = {
    post: [] as Array<ChatPostMessageArguments>,
    update: [] as Array<ChatUpdateArguments>,
    replies: [] as Array<ConversationsRepliesArguments>,
    history: [] as Array<ConversationsHistoryArguments>,
    open: [] as Array<ConversationsOpenArguments>,
    permalink: [] as Array<ChatGetPermalinkArguments>,
    ack: [] as Array<string>,
    add: [] as Array<ReactionsAddArguments>,
    remove: [] as Array<ReactionsRemoveArguments>,
  }
  const responses = {
    post: async (): Promise<unknown> => ({ ok: true, ts: plan.ts }),
    update: async (): Promise<unknown> => ({ ok: true }),
    replies: async (): Promise<unknown> => ({ ok: true, messages: [] }),
    history: async (): Promise<unknown> => ({ ok: true, messages: [] }),
    open: async (): Promise<unknown> => ({ ok: true, channel: { id: "D-OWNER" } }),
    permalink: async (): Promise<unknown> => ({
      ok: true,
      permalink: "https://example.slack.com/archives/C01/p1700000000000100",
    }),
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
      getPermalink: (input) => {
        calls.permalink.push(input)
        return responses.permalink()
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
      open: (input) => {
        calls.open.push(input)
        return responses.open()
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
        action: (name, handler) => {
          actions.set(name, handler)
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
    click: (name: string, body: unknown) =>
      Effect.promise(() =>
        actions.get(name)!({
          body,
          ack: async () => {
            calls.ack.push(name)
          },
        }),
      ),
    error: (cause: unknown) => Effect.promise(() => onError(cause)),
  }
}
