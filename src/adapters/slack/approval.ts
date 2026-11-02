import type { ChatUpdateArguments } from "@slack/web-api"
import { Effect, Schema } from "effect"
import type { Channel } from "../../ports/Channel.ts"
import { ChannelError } from "../../ports/Channel.ts"
import { call, escapeMrkdwn, metadata, Ok, Posted } from "./api.ts"
import type { SlackClient } from "./api.ts"

const Opened = Schema.Struct({ channel: Schema.Struct({ id: Schema.NonEmptyString }) })
const Permalink = Schema.Struct({ permalink: Schema.NonEmptyString })
const Mrkdwn = Schema.Struct({ type: Schema.Literal("mrkdwn"), text: Schema.String })
const ApprovalBlock = Schema.Union([
  Schema.Struct({ type: Schema.Literal("section"), text: Mrkdwn }),
  Schema.Struct({
    type: Schema.Literal("context"),
    block_id: Schema.optionalKey(Schema.String),
    elements: Schema.Array(Mrkdwn),
  }),
  Schema.Struct({ type: Schema.Literal("actions") }),
])
const ApprovalHistory = Schema.Struct({
  messages: Schema.Array(Schema.Struct({ ts: Schema.NonEmptyString, blocks: Schema.Array(ApprovalBlock) })),
})
const outcomeBlockId = "atta_approval_outcome"
// Slack refuses a section with more than 3000 characters of text.
const SECTION_LIMIT = 3000

/**
 * The brief as quoted sections, in full: the owner approves exactly what the worker will run. Escaping can
 * lengthen a line, so lines are packed into as many sections as they need.
 */
export const briefSections = (brief: string): ReadonlyArray<string> => {
  const sections: Array<string> = []
  let current = ""
  for (const line of escapeMrkdwn(brief).split("\n")) {
    let quoted = `>${line}`
    while (quoted.length > SECTION_LIMIT) {
      if (current !== "") sections.push(current)
      current = ""
      sections.push(quoted.slice(0, SECTION_LIMIT))
      quoted = `>${quoted.slice(SECTION_LIMIT)}`
    }
    const next = current === "" ? quoted : `${current}\n${quoted}`
    if (next.length > SECTION_LIMIT) {
      sections.push(current)
      current = quoted
    } else current = next
  }
  if (current !== "") sections.push(current)
  return sections
}
type Blocks = Extract<ChatUpdateArguments, { blocks: unknown }>["blocks"]

export const makeApprovalWriter = (
  client: SlackClient,
): Pick<Channel["Service"], "requestApproval" | "closeApproval"> => ({
  requestApproval: Effect.fn("Slack.requestApproval")(function* ({
    ownerUserId,
    proposalId,
    thread,
    repo,
    title,
    why,
    brief,
    operationId,
  }) {
    const dm = yield* call(
      "conversations.open",
      () => client.conversations.open({ users: ownerUserId }),
      Opened,
    )
    const link = yield* call(
      "chat.getPermalink",
      () => client.chat.getPermalink({ channel: thread.conversation, message_ts: thread.rootTs }),
      Permalink,
    )
    const section = `*${escapeMrkdwn(title)}*\n${escapeMrkdwn(why)}`
    const context = `${escapeMrkdwn(repo)} · <${link.permalink}|View thread>`
    const quotedBrief = briefSections(brief)
    // oxlint-disable unicorn/require-post-message-target-origin -- This is Slack's Web API.
    const posted = yield* call(
      "chat.postMessage",
      () =>
        client.chat.postMessage({
          channel: dm.channel.id,
          text: [section, ...quotedBrief, context].join("\n"),
          metadata: metadata(operationId),
          blocks: [
            { type: "section", text: { type: "mrkdwn", text: section } },
            ...quotedBrief.map((text) => ({
              type: "section" as const,
              text: { type: "mrkdwn" as const, text },
            })),
            { type: "context", elements: [{ type: "mrkdwn", text: context }] },
            {
              type: "actions",
              elements: [
                {
                  type: "button",
                  action_id: "atta_approve",
                  text: { type: "plain_text", text: "Start" },
                  style: "primary",
                  value: proposalId,
                },
                {
                  type: "button",
                  action_id: "atta_reject",
                  text: { type: "plain_text", text: "Skip" },
                  value: proposalId,
                },
              ],
            },
          ],
        }),
      Posted,
    )
    // oxlint-enable unicorn/require-post-message-target-origin
    return { team: thread.team, conversation: dm.channel.id, ts: posted.ts }
  }),
  closeApproval: Effect.fn("Slack.closeApproval")(function* ({ message, text }) {
    // Read Slack's copy so closing a proposal also works after atta restarts.
    const history = yield* call(
      "conversations.history",
      () =>
        client.conversations.history({
          channel: message.conversation,
          latest: message.ts,
          inclusive: true,
          limit: 1,
        }),
      ApprovalHistory,
    )
    const original = history.messages.find((entry) => entry.ts === message.ts)
    if (!original || !original.blocks.some((block) => block.type === "section")) {
      return yield* new ChannelError({ operation: "closeApproval", message: "Approval message not found" })
    }
    const blocks = original.blocks.flatMap<Blocks[number]>((block) => {
      if (block.type === "actions") return []
      if (block.type === "context") {
        return block.block_id === outcomeBlockId ? [] : [{ ...block, elements: [...block.elements] }]
      }
      return [block]
    })
    const outcome = escapeMrkdwn(text)
    yield* call(
      "chat.update",
      () =>
        client.chat.update({
          channel: message.conversation,
          ts: message.ts,
          text: [
            ...original.blocks.flatMap((block) => (block.type === "section" ? [block.text.text] : [])),
            outcome,
          ].join("\n"),
          blocks: [
            ...blocks,
            { type: "context", block_id: outcomeBlockId, elements: [{ type: "mrkdwn", text: outcome }] },
          ],
        }),
      Ok,
    )
  }),
})
