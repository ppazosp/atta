import { describe, expect, it } from "@effect/vitest"
import { Schema } from "effect"
import { parseThreadKey, ThreadKey, threadKey } from "./thread.ts"

describe("thread keys", () => {
  it("round-trips a thread", () => {
    const thread = { team: "T01", conversation: "C01", rootTs: "1700000000.000100" }
    expect(threadKey(thread)).toBe("slack:T01/C01/1700000000.000100")
    expect(parseThreadKey(threadKey(thread))).toEqual(thread)
  })

  it("rejects keys that are not Slack threads", () => {
    expect(Schema.is(ThreadKey)("slack:T01/C01")).toBe(false)
    expect(Schema.is(ThreadKey)("email:abc")).toBe(false)
  })
})
