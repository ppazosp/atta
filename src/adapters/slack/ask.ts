import { open, realpath } from "node:fs/promises"
import { basename, isAbsolute, relative } from "node:path"
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

const SIGNATURES = [
  [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a], // PNG
  [0xff, 0xd8, 0xff], // JPEG
  [0x47, 0x49, 0x46, 0x38], // GIF
]
const isImage = (file: Uint8Array) =>
  SIGNATURES.some((signature) => signature.every((byte, index) => file[index] === byte))

const Uploaded = Schema.Struct({
  files: Schema.Array(Schema.Struct({ files: Schema.Array(Schema.Struct({ id: Schema.NonEmptyString })) })),
})

/**
 * A worker's text as Markdown that cannot ping anyone or hide a link. Escaped `<`, `>` and `&` show as text, and a
 * backslash before every `[` stops inline and reference links, so only bare addresses link, and they show
 * themselves. All measured in Slack on 4 November 2026.
 */
export const askMarkdown = (text: string) =>
  // Backslashes first, so a worker's own `\\[` cannot cancel atta's escape.
  escapeMrkdwn(text).replaceAll("\\", "\\\\").replaceAll("[", "\\[")

/** A request that does not fit is refused, never cut: the user approves only what they could read in full. */
export const askFits = (text: string) => askMarkdown(text).length <= MARKDOWN_LIMIT

const refuse = (message: string) => new ChannelError({ operation: "askApproval", message })

/**
 * Reads one image of a request: inside the worker's worktree once symlinks are followed, a regular file, at most
 * 5 MB (never reading more), and an image by its content, since a file named chart.png could be anything.
 */
const readImage = Effect.fn("Slack.readImage")(function* (path: string, worktree: string | null) {
  const name = basename(path)
  if (worktree === null) return yield* refuse("this worker has no worktree to attach images from")
  const [root, real] = yield* Effect.tryPromise({
    try: () => Promise.all([realpath(worktree), realpath(path)]),
    catch: () => refuse(`Could not read ${name}`),
  })
  const inside = relative(root, real)
  if (inside === "" || inside.startsWith("..") || isAbsolute(inside)) {
    return yield* refuse(`${name} is outside your worktree`)
  }
  const file = yield* Effect.acquireUseRelease(
    Effect.tryPromise({ try: () => open(real, "r"), catch: () => refuse(`Could not read ${name}`) }),
    (handle) =>
      Effect.gen(function* () {
        const stat = yield* Effect.tryPromise({
          try: () => handle.stat(),
          catch: () => refuse(`Could not read ${name}`),
        })
        if (!stat.isFile()) return yield* refuse(`${name} is not a file`)
        const buffer = Buffer.alloc(MAX_IMAGE_BYTES + 1)
        const { bytesRead } = yield* Effect.tryPromise({
          try: () => handle.read(buffer, 0, buffer.length, 0),
          catch: () => refuse(`Could not read ${name}`),
        })
        if (bytesRead > MAX_IMAGE_BYTES) return yield* refuse(`${name} is over 5 MB`)
        return buffer.subarray(0, bytesRead)
      }),
    (handle) => Effect.promise(() => handle.close()),
  )
  if (!isImage(file)) return yield* refuse(`${name} is not a PNG, JPEG or GIF image`)
  return { file, filename: name }
})

const Thread = Schema.Struct({
  messages: Schema.Array(
    Schema.Struct({ ts: Schema.String, blocks: Schema.optionalKey(Schema.Array(Schema.Unknown)) }),
  ),
})
const ImageBlock = Schema.Struct({
  type: Schema.Literal("image"),
  slack_file: Schema.Struct({ id: Schema.NonEmptyString }),
})

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
): Pick<Channel["Service"], "askApproval" | "askFits" | "closeAsk"> => ({
  askFits,
  askApproval: Effect.fn("Slack.askApproval")(function* ({
    thread,
    askId,
    text,
    images,
    worktree,
    operationId,
  }) {
    if (!askFits(text)) return yield* refuse("The request is too long for one Slack message")
    // A retry after a crash finds the request it already posted, and its images by their file IDs in its blocks.
    const sent = yield* findReply({ target: { thread }, operationId })
    if (Option.isSome(sent)) {
      const read = yield* call(
        "conversations.replies",
        () =>
          client.conversations.replies({
            channel: thread.conversation,
            ts: thread.rootTs,
            oldest: sent.value.ts,
            latest: sent.value.ts,
            inclusive: true,
          }),
        Thread,
      )
      const blocks = read.messages.find((message) => message.ts === sent.value.ts)?.blocks ?? []
      const files = blocks.flatMap((block) =>
        Option.match(Schema.decodeUnknownOption(ImageBlock)(block), {
          onNone: () => [],
          onSome: (image) => [image.slack_file.id],
        }),
      )
      return { message: sent.value, files } satisfies AskMessage
    }
    const uploads = yield* Effect.forEach(images, (path) => readImage(path, worktree))
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
