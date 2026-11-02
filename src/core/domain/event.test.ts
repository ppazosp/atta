import { describe, expect, it } from "@effect/vitest"
import { Effect, Schema } from "effect"
import { Event } from "./event.ts"

const decode = Schema.decodeUnknownEffect(Event)

describe("events", () => {
  it.effect("decodes a Slack message from JSON", () =>
    Effect.gen(function* () {
      const event = yield* decode({
        _tag: "UserMessage",
        id: "e1",
        source: "slack",
        externalId: "Ev1",
        receivedAt: 1_700_000_000_000,
        thread: { team: "T01", conversation: "D01", rootTs: "1700000000.000100" },
        message: { team: "T01", conversation: "D01", ts: "1700000000.000100" },
        userId: "U01",
        text: "hello",
        via: "dm",
      })
      expect(event._tag).toBe("UserMessage")
    }),
  )

  it.effect("rejects an unknown kind", () =>
    Effect.gen(function* () {
      const result = yield* decode({ _tag: "Telegram", id: "e1" }).pipe(Effect.result)
      expect(result._tag).toBe("Failure")
    }),
  )
})
