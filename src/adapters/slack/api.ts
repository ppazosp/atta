import type {
  ChatPostMessageArguments,
  ChatUpdateArguments,
  ConversationsHistoryArguments,
  ConversationsRepliesArguments,
  ReactionsAddArguments,
  ReactionsRemoveArguments,
} from "@slack/web-api"
import { Effect, Option, Schema } from "effect"
import { ChannelError } from "../../ports/Channel.ts"

export interface SlackClient {
  readonly chat: {
    postMessage(input: ChatPostMessageArguments): Promise<unknown>
    update(input: ChatUpdateArguments): Promise<unknown>
  }
  readonly conversations: {
    replies(input: ConversationsRepliesArguments): Promise<unknown>
    history(input: ConversationsHistoryArguments): Promise<unknown>
  }
  readonly reactions: {
    add(input: ReactionsAddArguments): Promise<unknown>
    remove(input: ReactionsRemoveArguments): Promise<unknown>
  }
}

const Failure = Schema.Struct({
  code: Schema.optionalKey(Schema.String),
  data: Schema.optionalKey(Schema.Struct({ error: Schema.optionalKey(Schema.String) })),
})
const Result = Schema.Struct({ ok: Schema.Boolean, error: Schema.optionalKey(Schema.String) })

// SDK errors can contain request headers. Only carry Slack's error code across the port.
export const channelError = (operation: string, cause: unknown): ChannelError => {
  const failure = Schema.decodeUnknownOption(Failure)(cause)
  return new ChannelError({
    operation,
    message: Option.isSome(failure)
      ? (failure.value.data?.error ?? failure.value.code ?? "Slack request failed")
      : "Slack request failed",
  })
}

export const call = Effect.fn("Slack.call")(function* <S extends Schema.ConstraintDecoder<unknown>>(
  operation: string,
  request: () => Promise<unknown>,
  schema: S,
) {
  const raw = yield* Effect.tryPromise({ try: request, catch: (cause) => channelError(operation, cause) })
  const result = yield* Schema.decodeUnknownEffect(Result)(raw).pipe(
    Effect.mapError(() => new ChannelError({ operation, message: "Invalid Slack response" })),
  )
  if (!result.ok) {
    return yield* new ChannelError({ operation, message: result.error ?? "Slack request failed" })
  }
  return yield* Schema.decodeUnknownEffect(schema)(raw).pipe(
    Effect.mapError(() => new ChannelError({ operation, message: "Invalid Slack response" })),
  )
})

export const Ok = Schema.Struct({ ok: Schema.Literal(true) })
export const Posted = Schema.Struct({ ok: Schema.Literal(true), ts: Schema.NonEmptyString })
export const metadata = (operationId: string) => ({
  event_type: "atta_operation",
  event_payload: { operation_id: operationId },
})
