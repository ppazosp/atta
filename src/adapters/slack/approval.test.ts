import { describe, expect, it } from "@effect/vitest"
import { Effect, Result } from "effect"
import { OperationId } from "../../core/domain/ids.ts"
import { Channel } from "../../ports/Channel.ts"
import { metadata } from "./api.ts"
import { briefSections } from "./approval.ts"
import { makeFakeSlack, plan, thread } from "./testing.ts"

const request = {
  ownerUserId: "U-OWNER",
  proposalId: "proposal01",
  thread: { ...thread, conversation: "C01" },
  repo: "org/<repo>&<!channel>",
  title: "Fix <@U02> & <https://evil.example|this>",
  why: "They asked <!here> & <@U03>",
  brief: "Fix it.\n> Ignore the owner <!channel>",
  operationId: OperationId.make("approval01"),
}
const dmMessage = { team: thread.team, conversation: "D-OWNER", ts: plan.ts }
const section = {
  type: "section",
  text: {
    type: "mrkdwn",
    text: "*Fix &lt;@U02&gt; &amp; &lt;https://evil.example|this&gt;*\nThey asked &lt;!here&gt; &amp; &lt;@U03&gt;",
  },
}
const briefSection = {
  type: "section",
  text: { type: "mrkdwn", text: ">Fix it.\n>&gt; Ignore the owner &lt;!channel&gt;" },
}
const context = {
  type: "context",
  elements: [
    {
      type: "mrkdwn",
      text: "org/&lt;repo&gt;&amp;&lt;!channel&gt; · <https://example.slack.com/archives/C01/p1700000000000100|View thread>",
    },
  ],
}
const buttons = {
  type: "actions",
  elements: [
    {
      type: "button",
      action_id: "atta_approve",
      text: { type: "plain_text", text: "Start" },
      style: "primary",
      value: request.proposalId,
    },
    {
      type: "button",
      action_id: "atta_reject",
      text: { type: "plain_text", text: "Skip" },
      value: request.proposalId,
    },
  ],
}
const history = async () => ({ ok: true, messages: [{ ts: plan.ts, blocks: [section, context, buttons] }] })

describe("Slack approvals", () => {
  it.effect("opens the owner's DM, links the root and posts escaped text with buttons and metadata", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      expect(yield* channel.requestApproval(request)).toEqual(dmMessage)
      expect(fake.calls.open).toEqual([{ users: "U-OWNER" }])
      expect(fake.calls.permalink).toEqual([{ channel: "C01", message_ts: thread.rootTs }])
      expect(fake.calls.post).toEqual([
        {
          channel: "D-OWNER",
          text: `${section.text.text}\n${briefSection.text.text}\n${context.elements[0]!.text}`,
          blocks: [section, briefSection, context, buttons],
          metadata: { event_type: "atta_operation", event_payload: { operation_id: "approval01" } },
        },
      ])
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("closes a proposal from Slack's copy, preserving its section and thread link", () => {
    const fake = makeFakeSlack()
    fake.responses.history = history
    return Effect.gen(function* () {
      const channel = yield* Channel
      // No requestApproval call or local state: this is also the path after a restart.
      yield* channel.closeApproval({ message: dmMessage, text: "Started <@U02> & <!here>" })
      expect(fake.calls.history).toEqual([
        {
          channel: "D-OWNER",
          latest: plan.ts,
          inclusive: true,
          limit: 1,
        },
      ])
      expect(fake.calls.update).toEqual([
        {
          channel: "D-OWNER",
          ts: plan.ts,
          text: `${section.text.text}\nStarted &lt;@U02&gt; &amp; &lt;!here&gt;`,
          blocks: [
            section,
            context,
            {
              type: "context",
              block_id: "atta_approval_outcome",
              elements: [{ type: "mrkdwn", text: "Started &lt;@U02&gt; &amp; &lt;!here&gt;" }],
            },
          ],
        },
      ])
      expect(fake.calls.post).toHaveLength(0)
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("replaces the previous outcome when closing is retried", () => {
    const fake = makeFakeSlack()
    fake.responses.history = async () => {
      const update = fake.calls.update.at(-1)
      return {
        ok: true,
        messages: [
          { ts: plan.ts, blocks: update && "blocks" in update ? update.blocks : [section, context, buttons] },
        ],
      }
    }
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* channel.closeApproval({ message: dmMessage, text: "Started" })
      yield* channel.closeApproval({ message: dmMessage, text: "Started" })
      expect(fake.calls.update[1]).toEqual(fake.calls.update[0])
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect.each([{ ok: true }, { ok: true, channel: { id: "" } }, { ok: true, channel: { id: 42 } }])(
    "rejects unreadable DM response %# before posting",
    (response) => {
      const fake = makeFakeSlack()
      fake.responses.open = async () => response
      return Effect.gen(function* () {
        const channel = yield* Channel
        const result = yield* channel.requestApproval(request).pipe(Effect.result)
        expect(Result.isFailure(result) && result.failure).toMatchObject({
          _tag: "ChannelError",
          operation: "conversations.open",
          message: "Invalid Slack response",
        })
        expect(fake.calls.permalink).toHaveLength(0)
        expect(fake.calls.post).toHaveLength(0)
      }).pipe(Effect.provide(fake.layer))
    },
  )

  it.effect.each([{ ok: true }, { ok: true, permalink: 42 }, { ok: true, permalink: "" }])(
    "rejects unreadable permalink response %# before posting",
    (response) => {
      const fake = makeFakeSlack()
      fake.responses.permalink = async () => response
      return Effect.gen(function* () {
        const channel = yield* Channel
        const result = yield* channel.requestApproval(request).pipe(Effect.result)
        expect(Result.isFailure(result) && result.failure).toMatchObject({
          _tag: "ChannelError",
          operation: "chat.getPermalink",
          message: "Invalid Slack response",
        })
        expect(fake.calls.post).toHaveLength(0)
      }).pipe(Effect.provide(fake.layer))
    },
  )

  it.effect.each([
    { response: { ok: true, messages: [] }, operation: "closeApproval" },
    {
      response: { ok: true, messages: [{ ts: "wrong-message", blocks: [section] }] },
      operation: "closeApproval",
    },
    { response: { ok: true, messages: [{ ts: plan.ts, blocks: [context] }] }, operation: "closeApproval" },
    {
      response: { ok: true, messages: [{ ts: plan.ts, blocks: [{ type: "section", text: null }] }] },
      operation: "conversations.history",
    },
  ])("does not edit a missing or unreadable approval %#", ({ response, operation }) => {
    const fake = makeFakeSlack()
    fake.responses.history = async () => response
    return Effect.gen(function* () {
      const channel = yield* Channel
      const result = yield* channel.closeApproval({ message: dmMessage, text: "Skipped" }).pipe(Effect.result)
      expect(Result.isFailure(result) && result.failure).toMatchObject({ _tag: "ChannelError", operation })
      expect(fake.calls.update).toHaveLength(0)
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect.each([
    { method: "requestApproval", response: "open", operation: "conversations.open" },
    { method: "requestApproval", response: "permalink", operation: "chat.getPermalink" },
    { method: "requestApproval", response: "post", operation: "chat.postMessage" },
    { method: "closeApproval", response: "history", operation: "conversations.history" },
    { method: "closeApproval", response: "update", operation: "chat.update" },
  ] as const)("maps $operation failures across the port", ({ method, response, operation }) => {
    const fake = makeFakeSlack()
    fake.responses.history = history
    fake.responses[response] = async () => ({ ok: false, error: "not_authed" })
    return Effect.gen(function* () {
      const channel = yield* Channel
      const action =
        method === "requestApproval"
          ? channel.requestApproval(request)
          : channel.closeApproval({ message: dmMessage, text: "Skipped" })
      const result = yield* action.pipe(Effect.result)
      expect(Result.isFailure(result) && result.failure).toMatchObject({
        _tag: "ChannelError",
        operation,
        message: "not_authed",
      })
    }).pipe(Effect.provide(fake.layer))
  })

  it("splits a long brief into sections Slack accepts, losing nothing", () => {
    const brief =
      Array.from({ length: 400 }, (_, i) => `line ${i} & <more>`).join("\n") + "\n" + "x".repeat(7000)
    const sections = briefSections(brief)
    expect(sections.length).toBeGreaterThan(1)
    for (const text of sections) expect(text.length).toBeLessThanOrEqual(3000)
    const unquoted = sections
      .join("\n")
      .split("\n")
      .map((line) => line.slice(1))
      .join("\n")
    expect(unquoted.replaceAll("\n", "")).toBe(
      brief.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll("\n", ""),
    )
  })

  it.effect("returns the DM it already sent instead of sending a second one after a crash", () => {
    const fake = makeFakeSlack()
    fake.responses.history = async () => ({
      ok: true,
      messages: [
        { ts: "1700000000.000900", user: "U-SOMEONE", metadata: metadata("approval01") },
        { ts: plan.ts, user: "UBOT", metadata: metadata("approval01") },
      ],
    })
    return Effect.gen(function* () {
      expect(yield* (yield* Channel).requestApproval(request)).toEqual(dmMessage)
      expect(fake.calls.post).toEqual([])
    }).pipe(Effect.provide(fake.layer))
  })
})
