import { describe, expect, it } from "@effect/vitest"
import { Effect, Option, Result } from "effect"
import { OperationId } from "../../core/domain/ids.ts"
import { Channel } from "../../ports/Channel.ts"
import { metadata } from "./api.ts"
import { makeFakeSlack, plan, thread } from "./testing.ts"

const operationId = OperationId.make("op01")
const targets = [{ thread }, { conversation: { team: thread.team, conversation: thread.conversation } }]
const taggedReplies = async () => ({
  ok: true,
  messages: [
    { ts: "1" },
    { ts: "2", metadata: metadata("another-operation") },
    { ts: "3", metadata: { event_type: "something_else", event_payload: { operation_id: "op01" } } },
    { ts: "4", metadata: { event_type: "atta_operation" } },
    // Someone else's message carrying atta's tag must not count.
    { ts: "5", user: "U-SOMEONE", metadata: metadata("op01") },
    { ts: plan.ts, user: "UBOT", metadata: metadata("op01") },
  ],
})
const channelNotFound = () =>
  Promise.reject({ code: "slack_webapi_platform_error", data: { error: "channel_not_found" } })

describe("Slack Channel", () => {
  it.effect("starts Socket Mode and stops it when the layer closes", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      yield* Effect.gen(function* () {
        yield* Channel
        expect(fake.lifecycle.started).toBe(1)
        expect(fake.lifecycle.stopped).toBe(0)
        expect(fake.lifecycle.options).toMatchObject({ socketMode: true, appToken: "test-app-token" })
        const auth = yield* Effect.promise(() =>
          fake.lifecycle.options!.authorize!({
            isEnterpriseInstall: false,
            teamId: "T01",
            enterpriseId: undefined,
          }),
        )
        expect(auth).toEqual({ botToken: "test-bot-token", botUserId: "UBOT" })
      }).pipe(Effect.provide(fake.layer))
      expect(fake.lifecycle.stopped).toBe(1)
    })
  })

  it.effect("maps startup failures and still closes the app", () => {
    const fake = makeFakeSlack()
    fake.responses.start = () => Promise.reject({ code: "not_authed" })
    return Effect.gen(function* () {
      const result = yield* Channel.pipe(Effect.provide(fake.layer), Effect.result)
      expect(Result.isFailure(result) && result.failure).toMatchObject({
        _tag: "ChannelError",
        operation: "start",
        message: "not_authed",
      })
      expect(fake.lifecycle.stopped).toBe(1)
    })
  })

  it.effect.each(targets)("posts replies with operation metadata for target %#", (target) => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      expect(yield* channel.reply({ target, text: "answer", operationId })).toEqual(plan)
      expect(fake.calls.post).toEqual([
        {
          channel: thread.conversation,
          ...("thread" in target ? { thread_ts: thread.rootTs } : {}),
          text: "answer",
          metadata: { event_type: "atta_operation", event_payload: { operation_id: "op01" } },
        },
      ])
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect.each(targets)("finds a tagged reply with metadata requested for target %#", (target) => {
    const fake = makeFakeSlack()
    fake.responses.replies = taggedReplies
    fake.responses.history = taggedReplies
    return Effect.gen(function* () {
      const channel = yield* Channel
      expect(yield* channel.findReply({ target, operationId })).toEqual(Option.some(plan))
      const calls = "thread" in target ? fake.calls.replies : fake.calls.history
      expect(calls).toEqual([
        {
          channel: thread.conversation,
          include_all_metadata: true,
          limit: 100,
          ...("thread" in target ? { ts: thread.rootTs } : {}),
        },
      ])
      expect("thread" in target ? fake.calls.history : fake.calls.replies).toHaveLength(0)
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("follows replies cursors to find older operation metadata", () => {
    const fake = makeFakeSlack()
    fake.responses.replies = async () =>
      fake.calls.replies.length === 1
        ? { ok: true, messages: [], response_metadata: { next_cursor: "next-page" } }
        : { ok: true, messages: [{ ts: plan.ts, user: "UBOT", metadata: metadata("op01") }] }
    return Effect.gen(function* () {
      const channel = yield* Channel
      expect(yield* channel.findReply({ target: { thread }, operationId })).toEqual(Option.some(plan))
      expect(fake.calls.replies[1]).toMatchObject({ cursor: "next-page", include_all_metadata: true })
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("reads a thread's newest messages across pages, oldest first, and names no user for bots", () => {
    const fake = makeFakeSlack()
    fake.responses.replies = async () =>
      fake.calls.replies.length === 1
        ? {
            ok: true,
            messages: [
              { ts: "1", user: "U-ROOT", text: "root" },
              { ts: "2", user: "U-A", text: "a" },
            ],
            response_metadata: { next_cursor: "next-page" },
          }
        : {
            ok: true,
            messages: [
              { ts: "3", user: "UBOT", bot_id: "B1", text: "from atta" },
              { ts: "4", user: "U-B" },
            ],
          }
    return Effect.gen(function* () {
      const channel = yield* Channel
      expect(yield* channel.readConversation({ target: { thread }, limit: 3 })).toEqual([
        { userId: "U-A", ts: "2", text: "a" },
        { userId: null, ts: "3", text: "from atta" },
        { userId: "U-B", ts: "4", text: "" },
      ])
      expect(fake.calls.replies[1]).toMatchObject({ cursor: "next-page", ts: thread.rootTs })
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("reads a channel's latest messages oldest first", () => {
    const fake = makeFakeSlack()
    fake.responses.history = async () => ({
      ok: true,
      messages: [
        { ts: "2", user: "U-A", text: "newer" },
        { ts: "1", user: "U-B", text: "older" },
      ],
    })
    return Effect.gen(function* () {
      const channel = yield* Channel
      const target = { conversation: { team: thread.team, conversation: thread.conversation } }
      expect((yield* channel.readConversation({ target, limit: 2 })).map((m) => m.text)).toEqual([
        "older",
        "newer",
      ])
      expect(fake.calls.history[0]).toMatchObject({ channel: thread.conversation, limit: 2 })
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("ignores a tagged message that atta did not post", () => {
    const fake = makeFakeSlack()
    fake.responses.replies = async () => ({
      ok: true,
      messages: [{ ts: "9", user: "U-SOMEONE", metadata: metadata("op01") }],
    })
    return Effect.gen(function* () {
      const channel = yield* Channel
      expect(yield* channel.findReply({ target: { thread }, operationId })).toEqual(Option.none())
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("posts model text with Slack's special characters escaped", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* channel.reply({
        target: { thread },
        text: "<!channel> see <https://evil.example|docs> & go",
        operationId,
      })
      expect(fake.calls.post.at(-1)).toMatchObject({
        text: "&lt;!channel&gt; see &lt;https://evil.example|docs&gt; &amp; go",
      })
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect.each(targets)("returns None when target %# has no tagged reply", (target) => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      expect(yield* channel.findReply({ target, operationId })).toEqual(Option.none())
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect.each(["resolved", "rejected"])("treats repeated reactions as success (%s failures)", (mode) => {
    const fake = makeFakeSlack()
    fake.responses.add = () =>
      mode === "resolved"
        ? Promise.resolve({ ok: false, error: "already_reacted" })
        : Promise.reject({ code: "slack_webapi_platform_error", data: { error: "already_reacted" } })
    fake.responses.remove = () =>
      mode === "resolved"
        ? Promise.resolve({ ok: false, error: "no_reaction" })
        : Promise.reject({ code: "slack_webapi_platform_error", data: { error: "no_reaction" } })
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* channel.react(plan, "eyes")
      yield* channel.unreact(plan, "eyes")
      const expected = [{ channel: plan.conversation, timestamp: plan.ts, name: "eyes" }]
      expect(fake.calls.add).toEqual(expected)
      expect(fake.calls.remove).toEqual(expected)
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect.each(["reply", "findReply", "react", "unreact"] as const)("maps %s API errors", (method) => {
    const fake = makeFakeSlack()
    fake.responses.post = channelNotFound
    fake.responses.replies = channelNotFound
    fake.responses.add = channelNotFound
    fake.responses.remove = channelNotFound
    return Effect.gen(function* () {
      const channel = yield* Channel
      const action =
        method === "reply"
          ? channel.reply({ target: { thread }, text: "answer", operationId })
          : method === "findReply"
            ? channel.findReply({ target: { thread }, operationId })
            : channel[method](plan, "eyes")
      const result = yield* action.pipe(Effect.result)
      expect(Result.isFailure(result) && result.failure).toMatchObject({
        _tag: "ChannelError",
        message: "channel_not_found",
      })
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect.each([
    { ok: true },
    { ok: true, ts: 123 },
    { ok: "true", ts: "1" },
    { ok: false, error: "not_authed" },
  ])("rejects invalid or failed post response %#", (response) => {
    const fake = makeFakeSlack()
    fake.responses.post = async () => response
    return Effect.gen(function* () {
      const channel = yield* Channel
      const result = yield* channel
        .reply({ target: { thread }, text: "answer", operationId })
        .pipe(Effect.result)
      expect(Result.isFailure(result) && result.failure).toMatchObject({
        _tag: "ChannelError",
        operation: "chat.postMessage",
      })
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("does not expose request data or error messages containing secrets", () => {
    const fake = makeFakeSlack()
    fake.responses.post = () => Promise.reject(new Error("Authorization: test-secret-token"))
    return Effect.gen(function* () {
      const channel = yield* Channel
      const result = yield* channel
        .reply({ target: { thread }, text: "answer", operationId })
        .pipe(Effect.result)
      expect(Result.isFailure(result) && result.failure.message).toBe("Slack request failed")
    }).pipe(Effect.provide(fake.layer))
  })
})
