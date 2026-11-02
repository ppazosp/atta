import { describe, expect, it } from "@effect/vitest"
import { Effect, Result, Stream } from "effect"
import { TestClock } from "effect/testing"
import { Channel } from "../../ports/Channel.ts"
import { threadKey } from "../../core/domain/thread.ts"
import { envelope, makeFakeSlack, message, thread } from "./testing.ts"

describe("Slack events", () => {
  it.effect("records receipt time before the stream consumes the event", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* fake.emit("message", envelope(message))
      yield* TestClock.adjust(2_000)
      const [event] = yield* channel.events.pipe(Stream.take(1), Stream.runCollect)
      expect(event!.receivedAt).toBe(0)
    }).pipe(Effect.provide(fake.layer))
  })
  it.effect.each([
    { name: "DM", event: message, via: "dm", text: "hello", root: message.ts },
    {
      name: "DM reply",
      event: { ...message, ts: "2", thread_ts: message.ts },
      via: "dm",
      text: "hello",
      root: message.ts,
    },
    {
      name: "mention",
      event: { ...message, type: "app_mention", channel: "C01", text: "<@UBOT> hello <@U02> <@UBOT>" },
      via: "mention",
      text: "hello <@U02>",
      root: message.ts,
    },
    {
      name: "threaded mention",
      event: { ...message, type: "app_mention", ts: "2", thread_ts: message.ts, text: "hello <@UBOT>" },
      via: "mention",
      text: "hello",
      root: message.ts,
    },
    {
      name: "public thread reply",
      event: { ...message, channel_type: "channel", channel: "C01", ts: "2", thread_ts: message.ts },
      via: "thread_reply",
      text: "hello",
      root: message.ts,
    },
    {
      name: "private thread reply",
      event: { ...message, channel_type: "group", channel: "G01", ts: "2", thread_ts: message.ts },
      via: "thread_reply",
      text: "hello",
      root: message.ts,
    },
  ])("decodes $name", ({ event, via, text, root }) => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* fake.emit(event.type, envelope(event))
      const [actual] = yield* channel.events.pipe(Stream.take(1), Stream.runCollect)
      expect(actual).toMatchObject({
        _tag: "UserMessage",
        source: "slack",
        externalId: "Ev01",
        receivedAt: 0,
        thread: { team: "T01", conversation: event.channel, rootTs: root },
        message: { team: "T01", conversation: event.channel, ts: event.ts },
        userId: "U01",
        text,
        via,
      })
      expect(threadKey(actual!.thread)).toBe(`slack:T01/${event.channel}/${root}`)
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("keeps stable source, external ID and event ID for Slack retries", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* fake.emit("message", envelope(message))
      yield* fake.emit("message", envelope(message))
      yield* fake.emit("message", envelope(message, "Ev02"))
      const events = yield* channel.events.pipe(Stream.take(3), Stream.runCollect)
      expect(events.map((event) => [event.source, event.externalId])).toEqual([
        ["slack", "Ev01"],
        ["slack", "Ev01"],
        ["slack", "Ev02"],
      ])
      expect(events[0]!.id).toBe(events[1]!.id)
      expect(events[0]!.id).not.toBe(events[2]!.id)
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("ignores self, bot messages, every subtype, channel roots and unsupported conversations", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      const ignored = [
        { ...message, user: "UBOT" },
        { ...message, bot_id: "B01" },
        ...[
          "message_changed",
          "message_deleted",
          "bot_message",
          "channel_join",
          "thread_broadcast",
          "file_share",
        ].map((subtype) => ({ type: "message", subtype })),
        { ...message, subtype: "unknown_future_subtype" },
        { ...message, channel_type: "channel" },
        { ...message, channel_type: "group" },
        { ...message, channel_type: "mpim", thread_ts: thread.rootTs },
        { type: "app_home_opened" },
        { ...message, channel_type: "channel", thread_ts: thread.rootTs, text: "<@UBOT> hello" },
      ]
      for (const event of ignored) yield* fake.emit("message", envelope(event))
      yield* fake.emit(
        "app_mention",
        envelope({ ...message, type: "app_mention", text: "<@UBOT> hello" }, "EvMention"),
      )
      const [actual] = yield* channel.events.pipe(Stream.take(1), Stream.runCollect)
      expect(actual!.externalId).toBe("EvMention")
      expect(actual!.via).toBe("mention")
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect.each([
    { ...envelope(message), event_id: "" },
    { ...envelope(message), team_id: 42 },
    envelope({ ...message, ts: 123 }),
    envelope({ ...message, text: null }),
    envelope({ ...message, user: undefined }),
  ])("rejects malformed payload %# through ChannelError", (payload) => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* fake.emit("message", payload)
      const result = yield* channel.events.pipe(Stream.runHead, Effect.result)
      expect(Result.isFailure(result) && result.failure).toMatchObject({
        _tag: "ChannelError",
        operation: "events",
      })
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("maps Bolt errors into the events stream", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* fake.error({ code: "socket_failure" })
      const result = yield* channel.events.pipe(Stream.runHead, Effect.result)
      expect(Result.isFailure(result) && result.failure).toMatchObject({
        _tag: "ChannelError",
        operation: "events",
        message: "socket_failure",
      })
    }).pipe(Effect.provide(fake.layer))
  })
})
