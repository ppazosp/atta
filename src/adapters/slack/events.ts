import { Effect, Option, Schema } from "effect"
import { EventId, Timestamp } from "../../core/domain/ids.ts"
import type { Event } from "../../core/domain/event.ts"
import { ChannelError } from "../../ports/Channel.ts"

const Envelope = Schema.Struct({
  type: Schema.Literal("event_callback"),
  event_id: Schema.NonEmptyString,
  team_id: Schema.NonEmptyString,
  event: Schema.Unknown,
})
const Header = Schema.Struct({
  type: Schema.String,
  subtype: Schema.optionalKey(Schema.Unknown),
  bot_id: Schema.optionalKey(Schema.Unknown),
  user: Schema.optionalKey(Schema.String),
})
const Message = Schema.Struct({
  type: Schema.Literals(["message", "app_mention"]),
  channel: Schema.NonEmptyString,
  channel_type: Schema.optionalKey(Schema.String),
  user: Schema.NonEmptyString,
  ts: Schema.NonEmptyString,
  thread_ts: Schema.optionalKey(Schema.NonEmptyString),
  text: Schema.String,
})

const decode = <S extends Schema.ConstraintDecoder<unknown>>(schema: S, value: unknown) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError(() => new ChannelError({ operation: "events", message: "Invalid Slack event" })),
  )

export const decodeEvent = Effect.fn("Slack.decodeEvent")(function* (
  payload: unknown,
  botUserId: string,
  receivedAt: number,
): Effect.fn.Return<Option.Option<Extract<Event, { _tag: "UserMessage" }>>, ChannelError> {
  const body = yield* decode(Envelope, payload)
  const header = yield* decode(Header, body.event)
  if (
    (header.type !== "message" && header.type !== "app_mention") ||
    "subtype" in header ||
    "bot_id" in header ||
    header.user === botUserId
  ) {
    return Option.none()
  }
  const event = yield* decode(Message, body.event)
  const mention = `<@${botUserId}>`
  const dm = event.type === "message" && event.channel_type === "im"
  if (event.type === "message" && !dm) {
    // Slack also delivers a mentioned thread reply as app_mention, with its own event ID.
    if (!event.thread_ts || event.text.includes(mention)) return Option.none()
    if (event.channel_type !== "channel" && event.channel_type !== "group") return Option.none()
  }
  return Option.some({
    _tag: "UserMessage",
    id: EventId.make(`slack:${body.event_id}`),
    source: "slack",
    externalId: body.event_id,
    receivedAt: Timestamp.make(receivedAt),
    thread: { team: body.team_id, conversation: event.channel, rootTs: event.thread_ts ?? event.ts },
    message: { team: body.team_id, conversation: event.channel, ts: event.ts },
    userId: event.user,
    text: event.type === "app_mention" ? event.text.split(mention).join("").trim() : event.text,
    via: dm ? "dm" : event.type === "app_mention" ? "mention" : "thread_reply",
  })
})
