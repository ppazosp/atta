import { mkdtemp, symlink, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { describe, expect, it } from "@effect/vitest"
import { Effect, Fiber, Stream } from "effect"
import { TestClock } from "effect/testing"
import { OperationId } from "../../core/domain/ids.ts"
import { Channel } from "../../ports/Channel.ts"
import { ASK_APPROVE, ASK_CHANGE, askFits, askMarkdown, CHANGE_FORM, changeForm } from "./ask.ts"
import { makeFakeSlack, plan, thread } from "./testing.ts"

const request = (images: ReadonlyArray<string> = [], worktree: string | null = null) => ({
  thread,
  askId: "ask01",
  text: "## Plan\nUse option A.",
  images,
  worktree,
  operationId: OperationId.make("ask:ask01"),
})
const buttons = {
  type: "actions",
  elements: [
    {
      type: "button",
      action_id: ASK_APPROVE,
      text: { type: "plain_text", text: "Approve" },
      style: "primary",
      value: "ask01",
    },
    { type: "button", action_id: ASK_CHANGE, text: { type: "plain_text", text: "Change" }, value: "ask01" },
  ],
}
const image = (dir: string, name: string, bytes = 10) =>
  Effect.promise(async () => {
    const path = join(dir, name)
    // A PNG signature, then padding: atta checks the content, not the name.
    const png = Buffer.alloc(bytes)
    png.set([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a])
    await writeFile(path, png)
    return path
  })
const tempDir = Effect.promise(() => mkdtemp(join(tmpdir(), "atta-ask-")))

describe("Slack worker requests", () => {
  it("keeps a worker's Markdown from pinging anyone or hiding a link", () => {
    expect(askMarkdown("Hi <!channel> <@U01> & [docs](https://evil.example)")).toBe(
      "Hi &lt;!channel&gt; &lt;@U01&gt; &amp; \\[docs](https://evil.example)",
    )
    expect(askMarkdown("\\[x](https://evil.example)")).toBe("\\\\\\[x](https://evil.example)")
    expect(askMarkdown("[click][r]\n\n[r]: https://evil.example")).toBe(
      "\\[click]\\[r]\n\n\\[r]: https://evil.example",
    )
    // Counted as Slack counts it, after escaping: 3,000 `<` become 12,000 characters.
    expect(askFits("<".repeat(3000))).toBe(true)
    expect(askFits("<".repeat(3001))).toBe(false)
  })

  it.effect("uploads the images, then posts the text, the images and the buttons in one message", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const dir = yield* tempDir
      const chart = yield* image(dir, "chart.png")
      const posted = yield* (yield* Channel).askApproval(request([chart], dir))
      expect(posted).toEqual({ message: plan, files: ["F01"] })
      expect(fake.calls.upload).toHaveLength(1)
      expect(fake.calls.upload[0]).toMatchObject({ file_uploads: [{ filename: "chart.png" }] })
      expect(fake.calls.upload[0]).not.toHaveProperty("channel_id")
      expect(fake.calls.post).toEqual([
        {
          channel: thread.conversation,
          thread_ts: thread.rootTs,
          text: "## Plan",
          blocks: [
            { type: "markdown", text: "## Plan\nUse option A." },
            { type: "image", slack_file: { id: "F01" }, alt_text: "Image 1 of the request" },
            buttons,
          ],
          metadata: { event_type: "atta_operation", event_payload: { operation_id: "ask:ask01" } },
        },
      ])
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("retries the post while Slack is still processing an image", () => {
    const fake = makeFakeSlack()
    let attempts = 0
    fake.responses.post = async () => {
      attempts++
      return attempts < 3 ? { ok: false, error: "invalid_blocks" } : { ok: true, ts: plan.ts }
    }
    return Effect.gen(function* () {
      const dir = yield* tempDir
      const chart = yield* image(dir, "chart.png")
      const posting = yield* (yield* Channel).askApproval(request([chart], dir)).pipe(Effect.forkChild)
      // Reading the image is real I/O, so the clock moves a second at a time until the post lands.
      for (let tick = 0; tick < 50 && posting.pollUnsafe() === undefined; tick++) {
        yield* Effect.promise(() => new Promise((resolve) => setImmediate(resolve)))
        yield* TestClock.adjust("1 second")
      }
      expect((yield* Fiber.join(posting)).message).toEqual(plan)
      expect(attempts).toBe(3)
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("refuses a bad image, or text that does not fit, before uploading or posting anything", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const dir = yield* tempDir
      const big = yield* image(dir, "big.png", 5 * 1024 * 1024 + 1)
      const channel = yield* Channel
      expect((yield* channel.askApproval(request([big], dir)).pipe(Effect.flip)).message).toBe(
        "big.png is over 5 MB",
      )
      const missing = join(dir, "missing.png")
      expect((yield* channel.askApproval(request([missing], dir)).pipe(Effect.flip)).message).toBe(
        "Could not read missing.png",
      )
      const secret = join(dir, "env")
      yield* Effect.promise(() => writeFile(secret, "SLACK_BOT_TOKEN=x"))
      const disguised = join(dir, "chart.png")
      yield* Effect.promise(() => symlink(secret, disguised))
      expect((yield* channel.askApproval(request([disguised], dir)).pipe(Effect.flip)).message).toBe(
        "chart.png is not a PNG, JPEG or GIF image",
      )
      // A real image outside the worktree, reached through a symlink inside it.
      const elsewhere = yield* image(yield* tempDir, "private.png")
      const link = join(dir, "link.png")
      yield* Effect.promise(() => symlink(elsewhere, link))
      expect((yield* channel.askApproval(request([link], dir)).pipe(Effect.flip)).message).toBe(
        "link.png is outside your worktree",
      )
      expect((yield* channel.askApproval(request([link])).pipe(Effect.flip)).message).toBe(
        "this worker has no worktree to attach images from",
      )
      const tooLong = { ...request(), text: "<".repeat(3001) }
      expect((yield* channel.askApproval(tooLong).pipe(Effect.flip)).message).toBe(
        "The request is too long for one Slack message",
      )
      expect(fake.calls.upload).toEqual([])
      expect(fake.calls.post).toEqual([])
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("returns the request it already posted, with its images, instead of posting it again", () => {
    const fake = makeFakeSlack()
    fake.responses.replies = async () => ({
      ok: true,
      messages: [
        {
          ts: plan.ts,
          user: "UBOT",
          bot_id: "B01",
          metadata: { event_type: "atta_operation", event_payload: { operation_id: "ask:ask01" } },
          blocks: [
            { type: "rich_text", elements: [] },
            { type: "image", slack_file: { id: "F09" }, image_url: "https://files.slack.com/x.png" },
            { type: "actions", elements: [] },
          ],
        },
      ],
    })
    return Effect.gen(function* () {
      // Its images come back by their file IDs, so closing the request keeps them.
      expect(yield* (yield* Channel).askApproval(request())).toEqual({ message: plan, files: ["F09"] })
      expect(fake.calls.post).toEqual([])
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("closes a request: the same text and images, with the outcome in place of the buttons", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      yield* (yield* Channel).closeAsk({
        ask: { message: plan, files: ["F01"] },
        text: "## Plan\nUse option A.",
        outcome: "Changes asked: <!channel> use B",
      })
      expect(fake.calls.update).toEqual([
        {
          channel: plan.conversation,
          ts: plan.ts,
          text: "## Plan",
          blocks: [
            { type: "markdown", text: "## Plan\nUse option A." },
            { type: "image", slack_file: { id: "F01" }, alt_text: "Image 1 of the request" },
            {
              type: "context",
              elements: [{ type: "mrkdwn", text: "Changes asked: &lt;!channel&gt; use B" }],
            },
          ],
        },
      ])
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("turns Approve into an answer, and opens the form on Change", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* fake.click(ASK_CHANGE, {
        type: "block_actions",
        trigger_id: "trigger01",
        user: { id: "U01" },
        actions: [{ type: "button", action_id: ASK_CHANGE, action_ts: "1", value: "ask01" }],
      })
      expect(fake.calls.views).toEqual([{ trigger_id: "trigger01", view: changeForm("ask01") }])
      yield* fake.click(ASK_APPROVE, {
        type: "block_actions",
        user: { id: "U01" },
        actions: [{ type: "button", action_id: ASK_APPROVE, action_ts: "1700000003.000100", value: "ask01" }],
      })
      yield* fake.click(CHANGE_FORM, {
        type: "view_submission",
        user: { id: "U01" },
        view: {
          id: "V01",
          callback_id: CHANGE_FORM,
          private_metadata: "ask01",
          state: { values: { change: { text: { type: "plain_text_input", value: "Use option B" } } } },
        },
      })
      expect(fake.calls.ack).toEqual([ASK_CHANGE, ASK_APPROVE, CHANGE_FORM])
      const events = yield* channel.events.pipe(Stream.take(2), Stream.runCollect)
      expect(events).toEqual([
        {
          _tag: "WorkerAnswer",
          id: "slack:answer:ask01:1700000003.000100",
          source: "slack",
          externalId: "answer:ask01:U01",
          receivedAt: expect.any(Number),
          askId: "ask01",
          decision: "approve",
          text: null,
          userId: "U01",
        },
        {
          _tag: "WorkerAnswer",
          id: "slack:answer:ask01:V01",
          source: "slack",
          externalId: "answer:ask01:U01",
          receivedAt: expect.any(Number),
          askId: "ask01",
          decision: "change",
          text: "Use option B",
          userId: "U01",
        },
      ])
    }).pipe(Effect.provide(fake.layer))
  })
})
