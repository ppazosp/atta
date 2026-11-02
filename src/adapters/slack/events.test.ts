import { describe, expect, it } from "@effect/vitest"
import { Effect, Option, Result, Stream } from "effect"
import { TestClock } from "effect/testing"
import { Channel } from "../../ports/Channel.ts"
import { threadKey } from "../../core/domain/thread.ts"
import { buttonAction, envelope, makeFakeSlack, message, thread } from "./testing.ts"

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
      event: {
        ...message,
        type: "app_mention",
        channel_type: "channel",
        channel: "C01",
        text: "<@UBOT> hello <@U02> <@UBOT>",
      },
      via: "mention",
      text: "hello <@U02>",
      root: message.ts,
    },
    ...["channel", "group"].map((channelType) => ({
      name: `${channelType} root`,
      event: { ...message, channel_type: channelType, channel: channelType === "channel" ? "C01" : "G01" },
      via: "channel",
      text: "hello",
      root: message.ts,
    })),
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
      expect(actual!._tag).toBe("UserMessage")
      if (actual!._tag === "UserMessage") {
        expect(threadKey(actual!.thread)).toBe(`slack:T01/${event.channel}/${root}`)
      }
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

  it.effect("ignores self, bot messages, every subtype and unsupported conversations", () => {
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
        { ...message, channel_type: "channel", text: "<@UBOT> hello" },
        { ...message, channel_type: "group", text: "<@UBOT> hello" },
        { ...message, channel_type: "mpim" },
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
      expect(actual!._tag === "UserMessage" && actual!.via).toBe("mention")
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect.each([
    { actionId: "atta_approve", decision: "approve" },
    { actionId: "atta_reject", decision: "reject" },
  ])("acknowledges $actionId immediately and emits a decision", ({ actionId, decision }) => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* TestClock.adjust(123)
      yield* fake.click(actionId, buttonAction(actionId))
      expect(fake.calls.ack).toEqual([actionId])
      yield* TestClock.adjust(2_000)
      const [event] = yield* channel.events.pipe(Stream.take(1), Stream.runCollect)
      expect(event).toEqual({
        _tag: "ApprovalDecision",
        id: "slack:decision:proposal01:1700000002.000100",
        source: "slack",
        externalId: "decision:proposal01:U01",
        receivedAt: 123,
        proposalId: "proposal01",
        decision,
        userId: "U01",
      })
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("keeps one deduplication ID per proposal and clicker across decisions and retries", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* fake.click("atta_approve", buttonAction("atta_approve", "proposal01", "1"))
      yield* fake.click("atta_approve", buttonAction("atta_approve", "proposal01", "1"))
      yield* fake.click("atta_reject", buttonAction("atta_reject", "proposal01", "2"))
      const events = yield* channel.events.pipe(Stream.take(3), Stream.runCollect)
      expect(events.map((event) => event.externalId)).toEqual(Array(3).fill("decision:proposal01:U01"))
      expect(events.map((event) => event.id)).toEqual([
        "slack:decision:proposal01:1",
        "slack:decision:proposal01:1",
        "slack:decision:proposal01:2",
      ])
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect.each([
    null,
    { ...buttonAction("atta_approve"), user: { id: "" } },
    { ...buttonAction("atta_approve"), actions: [] },
    buttonAction("atta_approve", ""),
    buttonAction("atta_reject", "proposal01", ""),
    buttonAction("unknown_action"),
    {
      ...buttonAction("atta_approve"),
      actions: [{ type: "button", action_id: "atta_approve", action_ts: "1", value: 42 }],
    },
  ])("acknowledges and skips unreadable action %#", (payload) => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* fake.click("atta_approve", payload)
      expect(fake.calls.ack).toEqual(["atta_approve"])
      yield* fake.emit("message", envelope(message, "Ev-after-action"))
      const [event] = yield* channel.events.pipe(Stream.take(1), Stream.runCollect)
      expect(event!.externalId).toBe("Ev-after-action")
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect.each([
    { ...envelope(message), event_id: "" },
    { ...envelope(message), team_id: 42 },
    envelope({ ...message, ts: 123 }),
    envelope({ ...message, text: null }),
    envelope({ ...message, user: undefined }),
  ])("skips malformed payload %# and keeps delivering later events", (payload) => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* fake.emit("message", payload)
      yield* fake.emit("message", envelope({ ...message, channel_type: "im" }, "Ev-after"))
      const first = yield* channel.events.pipe(Stream.runHead)
      expect(Option.getOrThrow(first).externalId).toBe("Ev-after")
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
