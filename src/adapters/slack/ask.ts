import { readFile } from "node:fs/promises"
import { basename } from "node:path"
import { Effect, Option, Schedule, Schema } from "effect"
import type { AskMessage, Channel } from "../../ports/Channel.ts"
import { ChannelError } from "../../ports/Channel.ts"
import { call, escapeMrkdwn, metadata, Ok, Posted } from "./api.ts"
import type { SlackClient } from "./api.ts"

export const ASK_APPROVE = "atta_ask_approve"
export const ASK_CHANGE = "atta_ask_change"
/** The form Change opens; its submission carries what to change. */
export const CHANGE_FORM = "atta_ask_change_form"
export const CHANGE_BLOCK = "change"
export const CHANGE_INPUT = "text"

// Slack refuses a Markdown message over 12,000 characters (measured on 4 November 2026).
const MARKDOWN_LIMIT = 12_000
const MAX_IMAGE_BYTES = 5 * 1024 * 1024

const Uploaded = Schema.Struct({
  files: Schema.Array(Schema.Struct({ files: Schema.Array(Schema.Struct({ id: Schema.NonEmptyString })) })),
})

/**
 * A worker's text as Markdown that cannot ping anyone or hide a link. Escaped `<`, `>` and `&` show as text, and
 * `](` is broken up, so a link shows its address. Both measured in Slack on 4 November 2026.
 */
export const askMarkdown = (text: string) => {
  const safe = escapeMrkdwn(text).replaceAll("](", "] (")
  return safe.length > MARKDOWN_LIMIT
    ? `${safe.slice(0, MARKDOWN_LIMIT - 40)}\n\n(cut at Slack's limit)`
    : safe
}

type Block = Record<string, unknown>

const askBlocks = (text: string, files: ReadonlyArray<string>, tail: Block): ReadonlyArray<Block> => [
  { type: "markdown", text: askMarkdown(text) },
  ...files.map((id, index) => ({
    type: "image",
    slack_file: { id },
    alt_text: `Image ${index + 1} of the request`,
  })),
  tail,
]

const buttons = (askId: string): Block => ({
  type: "actions",
  elements: [
    {
      type: "button",
      action_id: ASK_APPROVE,
      text: { type: "plain_text", text: "Approve" },
      style: "primary",
      value: askId,
    },
    { type: "button", action_id: ASK_CHANGE, text: { type: "plain_text", text: "Change" }, value: askId },
  ],
})

const outcomeBlock = (outcome: string): Block => ({
  type: "context",
  elements: [{ type: "mrkdwn", text: escapeMrkdwn(outcome).slice(0, 3000) }],
})

/** The first line, for notifications and clients that cannot show blocks. */
const fallback = (text: string) =>
  escapeMrkdwn(text.split("\n").find((line) => line.trim() !== "") ?? "").slice(0, 200)

/** The form that asks what to change. Submitting it answers the request. */
export const changeForm = (askId: string) => ({
  type: "modal" as const,
  callback_id: CHANGE_FORM,
  private_metadata: askId,
  title: { type: "plain_text" as const, text: "Ask for changes" },
  submit: { type: "plain_text" as const, text: "Send" },
  close: { type: "plain_text" as const, text: "Cancel" },
  blocks: [
    {
      type: "input" as const,
      block_id: CHANGE_BLOCK,
      label: { type: "plain_text" as const, text: "What should change?" },
      element: { type: "plain_text_input" as const, action_id: CHANGE_INPUT, multiline: true },
    },
  ],
})

export const makeAskWriter = (
  client: SlackClient,
  findReply: Channel["Service"]["findReply"],
): Pick<Channel["Service"], "askApproval" | "closeAsk"> => ({
  askApproval: Effect.fn("Slack.askApproval")(function* ({ thread, askId, text, images, operationId }) {
    // A retry after a crash finds the request it already posted. Its images then stay off the closed request.
    const sent = yield* findReply({ target: { thread }, operationId })
    if (Option.isSome(sent)) return { message: sent.value, files: [] } satisfies AskMessage
    const uploads = yield* Effect.forEach(images, (path) =>
      Effect.tryPromise({
        try: () => readFile(path),
        catch: () =>
          new ChannelError({ operation: "askApproval", message: `Could not read ${basename(path)}` }),
      }).pipe(
        Effect.filterOrFail(
          (file) => file.length <= MAX_IMAGE_BYTES,
          () => new ChannelError({ operation: "askApproval", message: `${basename(path)} is over 5 MB` }),
        ),
        Effect.map((file) => ({ file, filename: basename(path) })),
      ),
    )
    // Uploaded without a channel, so the images show only inside the request message.
    const files =
      uploads.length === 0
        ? []
        : (yield* call(
            "files.uploadV2",
            () => client.files.uploadV2({ file_uploads: uploads }),
            Uploaded,
          )).files.flatMap((upload) => upload.files.map((file) => file.id))
    // oxlint-disable unicorn/require-post-message-target-origin -- This is Slack's Web API.
    const posted = yield* call(
      "chat.postMessage",
      () =>
        client.chat.postMessage({
          channel: thread.conversation,
          thread_ts: thread.rootTs,
          text: fallback(text),
          blocks: askBlocks(text, files, buttons(askId)) as never,
          metadata: metadata(operationId),
        }),
      Posted,
    ).pipe(
      // Slack refuses an image block until it has processed the file, which takes a moment after the upload.
      Effect.retry({
        while: (error) => files.length > 0 && error.message === "invalid_blocks",
        schedule: Schedule.spaced("1 second"),
        times: 5,
      }),
    )
    // oxlint-enable unicorn/require-post-message-target-origin
    return {
      message: { team: thread.team, conversation: thread.conversation, ts: posted.ts },
      files,
    } satisfies AskMessage
  }),
  closeAsk: Effect.fn("Slack.closeAsk")(function* ({ ask, text, outcome }) {
    yield* call(
      "chat.update",
      () =>
        client.chat.update({
          channel: ask.message.conversation,
          ts: ask.message.ts,
          text: fallback(text),
          blocks: askBlocks(text, ask.files, outcomeBlock(outcome)) as never,
        }),
      Ok,
    )
  }),
})
