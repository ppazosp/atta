import { Schema } from "effect"

/** A Slack conversation: a DM or a channel, in one workspace. */
export const Conversation = Schema.Struct({
  team: Schema.NonEmptyString,
  conversation: Schema.NonEmptyString,
})
export type Conversation = typeof Conversation.Type

/** One message in Slack, the target of reactions and edits. */
export const MessageRef = Schema.Struct({
  team: Schema.NonEmptyString,
  conversation: Schema.NonEmptyString,
  ts: Schema.NonEmptyString,
})
export type MessageRef = typeof MessageRef.Type

/** A Slack thread: the conversation plus the timestamp of the message that started it. */
export const ThreadRef = Schema.Struct({
  team: Schema.NonEmptyString,
  conversation: Schema.NonEmptyString,
  rootTs: Schema.NonEmptyString,
})
export type ThreadRef = typeof ThreadRef.Type

/** The string form of a thread, used as a key in the store: `slack:<team>/<conversation>/<rootTs>`. */
export const ThreadKey = Schema.String.check(Schema.isPattern(/^slack:[^/]+\/[^/]+\/[^/]+$/)).pipe(
  Schema.brand("ThreadKey"),
)
export type ThreadKey = typeof ThreadKey.Type

export const threadKey = (thread: ThreadRef): ThreadKey =>
  ThreadKey.make(`slack:${thread.team}/${thread.conversation}/${thread.rootTs}`)

export const parseThreadKey = (key: ThreadKey): ThreadRef => {
  const [team = "", conversation = "", rootTs = ""] = key.slice("slack:".length).split("/")
  return { team, conversation, rootTs }
}
