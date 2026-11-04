import { Effect, Option, Schema } from "effect"
import { EventId, Timestamp } from "../../core/domain/ids.ts"
import type { Event } from "../../core/domain/event.ts"
import { ChannelError } from "../../ports/Channel.ts"
import { ASK_APPROVE, CHANGE_BLOCK, CHANGE_FORM, CHANGE_INPUT } from "./ask.ts"

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
const Action = Schema.Struct({
  type: Schema.Literal("block_actions"),
  user: Schema.Struct({ id: Schema.NonEmptyString }),
  actions: Schema.Tuple([
    Schema.Struct({
      type: Schema.Literal("button"),
      action_id: Schema.Literals(["atta_approve", "atta_reject"]),
      action_ts: Schema.NonEmptyString,
      value: EventId,
    }),
  ]),
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
    // Slack also delivers a mention as app_mention, with its own event ID.
    if (event.text.includes(mention)) return Option.none()
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
    via: dm ? "dm" : event.type === "app_mention" ? "mention" : event.thread_ts ? "thread_reply" : "channel",
  })
})

export const decodeAction = Effect.fn("Slack.decodeAction")(function* (
  payload: unknown,
  receivedAt: number,
): Effect.fn.Return<Option.Option<Extract<Event, { _tag: "ApprovalDecision" }>>, ChannelError> {
  const body = yield* decode(Action, payload)
  const [action] = body.actions
  return Option.some({
    _tag: "ApprovalDecision",
    id: EventId.make(`slack:decision:${action.value}:${action.action_ts}`),
    source: "slack",
    // Per clicker, so someone else's click, which core drops, cannot use up the owner's.
    externalId: `decision:${action.value}:${body.user.id}`,
    receivedAt: Timestamp.make(receivedAt),
    proposalId: action.value,
    decision: action.action_id === "atta_approve" ? "approve" : "reject",
    userId: body.user.id,
  })
})

const Approve = Schema.Struct({
  type: Schema.Literal("block_actions"),
  user: Schema.Struct({ id: Schema.NonEmptyString }),
  actions: Schema.Tuple([
    Schema.Struct({
      type: Schema.Literal("button"),
      action_id: Schema.Literal(ASK_APPROVE),
      action_ts: Schema.NonEmptyString,
      value: EventId,
    }),
  ]),
})
const ChangeSubmitted = Schema.Struct({
  type: Schema.Literal("view_submission"),
  user: Schema.Struct({ id: Schema.NonEmptyString }),
  view: Schema.Struct({
    id: Schema.NonEmptyString,
    callback_id: Schema.Literal(CHANGE_FORM),
    private_metadata: EventId,
    state: Schema.Struct({
      values: Schema.Struct({
        [CHANGE_BLOCK]: Schema.Struct({
          [CHANGE_INPUT]: Schema.Struct({ value: Schema.NullOr(Schema.String) }),
        }),
      }),
    }),
  }),
})

/** Approve on a worker's request, or the Change form sent. Either answers the request. */
export const decodeAnswer = Effect.fn("Slack.decodeAnswer")(function* (
  payload: unknown,
  receivedAt: number,
): Effect.fn.Return<Option.Option<Extract<Event, { _tag: "WorkerAnswer" }>>, ChannelError> {
  const answer = yield* decode(Schema.Union([Approve, ChangeSubmitted]), payload)
  const [askId, decision, text, at] =
    answer.type === "block_actions"
      ? ([answer.actions[0].value, "approve", null, answer.actions[0].action_ts] as const)
      : ([
          answer.view.private_metadata,
          "change",
          answer.view.state.values[CHANGE_BLOCK][CHANGE_INPUT].value ?? "",
          answer.view.id,
        ] as const)
  return Option.some({
    _tag: "WorkerAnswer",
    id: EventId.make(`slack:answer:${askId}:${at}`),
    source: "slack",
    // Per user, so only the first answer counts, and someone else's, which core drops, cannot use up the owner's.
    externalId: `answer:${askId}:${answer.user.id}`,
    receivedAt: Timestamp.make(receivedAt),
    askId,
    decision,
    text,
    userId: answer.user.id,
  })
})
