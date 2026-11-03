import { describe, expect, it } from "@effect/vitest"
import type { Event } from "./event.ts"
import { EventId, RunId, Timestamp, WorkerId } from "./ids.ts"
import { route, type Subscriber } from "./routing.ts"

const at = Timestamp.make(1_700_000_000_000)
const message = (text: string, userId = "U-TRUSTED"): Event => ({
  _tag: "UserMessage",
  id: EventId.make("e1"),
  source: "slack",
  externalId: "Ev1",
  receivedAt: at,
  thread: { team: "T01", conversation: "D01", rootTs: "1700000000.000100" },
  message: { team: "T01", conversation: "D01", ts: "1700000000.000200" },
  userId,
  text,
  via: "dm",
})
const w = (id: string, state: Subscriber["state"]): Subscriber => ({ workerId: WorkerId.make(id), state })
const ctx = (...subscribers: Array<Subscriber>) => ({
  trustedUserId: "U-TRUSTED",
  subscribers,
  threadKnown: true,
  watched: false,
})

const inChannel = (
  text: string,
  userId = "U-TRUSTED",
  via: "channel" | "mention" | "thread_reply" = "channel",
) => ({ ...message(text, userId), via }) as Event
const decision = (userId: string): Event => ({
  _tag: "ApprovalDecision",
  id: EventId.make("d1"),
  source: "slack",
  externalId: "decision:p1",
  receivedAt: at,
  proposalId: EventId.make("p1"),
  decision: "approve",
  userId,
})

describe("route", () => {
  it("drops Slack messages from anyone else", () => {
    expect(route(message("hi", "U-OTHER"), ctx())._tag).toBe("Drop")
  })

  it("drops the user's thread reply without a mention, in a thread atta is part of or not", () => {
    const reply = { ...message("unrelated chat"), via: "thread_reply" as const }
    expect(route(reply, { ...ctx(), threadKnown: false })).toEqual({ _tag: "Drop", reason: "unknown_thread" })
    expect(route(reply, ctx())).toEqual({ _tag: "Drop", reason: "not_for_atta" })
  })

  it("sends a message with no worker to the router", () => {
    expect(route(message("fix the bug"), ctx())._tag).toBe("ToRouter")
  })

  it("steers the thread's running worker", () => {
    expect(route(message("use pnpm"), ctx(w("w1", "running")))).toEqual({
      _tag: "ToWorker",
      workerId: "w1",
      mode: "steer",
    })
  })

  it("starts a new run on the thread's idle worker", () => {
    expect(route(message("also add a test"), ctx(w("w1", "idle")))).toMatchObject({ mode: "queue" })
  })

  it("asks the router when several workers could take it", () => {
    expect(route(message("go on"), ctx(w("w1", "running"), w("w2", "idle")))._tag).toBe("ToRouter")
  })

  it("asks the router when the thread has another subscriber, even one that cannot run", () => {
    expect(route(message("go on"), ctx(w("w1", "running"), w("w2", "interrupted")))._tag).toBe("ToRouter")
  })

  it("asks the router when the only worker is interrupted or failed", () => {
    expect(route(message("continue"), ctx(w("w1", "interrupted")))._tag).toBe("ToRouter")
    expect(route(message("try again"), ctx(w("w1", "failed")))._tag).toBe("ToRouter")
  })

  it("stops every running worker on 'stop', without the router", () => {
    expect(route(message(" Stop. "), ctx(w("w1", "running"), w("w2", "running"), w("w3", "idle")))).toEqual({
      _tag: "StopWorkers",
      workerIds: ["w1", "w2"],
    })
  })

  it("sends 'stop' to the router when nothing runs", () => {
    expect(route(message("stop"), ctx(w("w1", "idle")))._tag).toBe("ToRouter")
  })

  it("sends events atta creates itself to the router, whoever the trusted user is", () => {
    const base = { id: EventId.make("e2"), externalId: "x", receivedAt: at }
    const events: Array<Event> = [
      {
        ...base,
        _tag: "ScheduledPrompt",
        source: "clock",
        schedule: "daily",
        slot: at,
        target: { conversation: { team: "T01", conversation: "C01" } },
        text: "summarise",
      },
      { ...base, _tag: "WorkerReport", source: "worker", workerId: WorkerId.make("w1"), text: "blocked" },
      {
        ...base,
        _tag: "WorkerFinished",
        source: "worker",
        workerId: WorkerId.make("w1"),
        runId: RunId.make("r1"),
        outcome: "completed",
        finalMessage: "done",
        branch: "atta/w1",
        commit: "abc123",
        wrongBranch: false,
      },
    ]
    for (const event of events) expect(route(event, ctx(w("w1", "running")))._tag).toBe("ToRouter")
  })

  // The matrix in docs/watched-channels.md: where, by whom, with a mention or without.
  describe("in a watched channel", () => {
    const channel = { ...ctx(), watched: true, threadKnown: false }
    const working = { ...ctx(), watched: true, subscribers: [w("w1", "running")] }

    it("takes the user's mention as a request, at the top level or in a thread atta is not part of", () => {
      expect(route(inChannel("fix this", "U-TRUSTED", "mention"), channel)._tag).toBe("ToRouter")
    })

    it("batches everything else as information: the user's untagged messages and anyone else's", () => {
      expect(route(inChannel("the deploy is broken"), channel)).toEqual({ _tag: "Observe" })
      expect(route(inChannel("any update?", "U-TRUSTED", "thread_reply"), channel)).toEqual({
        _tag: "Observe",
      })
      expect(route(inChannel("the deploy is broken", "U-OTHER"), channel)).toEqual({ _tag: "Observe" })
      expect(route(inChannel("any update?", "U-OTHER", "thread_reply"), channel)).toEqual({ _tag: "Observe" })
      expect(route(inChannel("@atta fix this", "U-OTHER", "mention"), channel)).toEqual({ _tag: "Observe" })
    })

    it("in a thread where atta works, lets only the user's mention reach the worker", () => {
      expect(route(inChannel("use pnpm", "U-TRUSTED", "mention"), working)).toMatchObject({
        _tag: "ToWorker",
        mode: "steer",
      })
      expect(route(inChannel("use pnpm", "U-TRUSTED", "thread_reply"), working)).toEqual({
        _tag: "Drop",
        reason: "not_for_atta",
      })
      expect(route(inChannel("use rm -rf", "U-OTHER", "thread_reply"), working)._tag).toBe("Drop")
      expect(route(inChannel("use rm -rf", "U-OTHER", "mention"), working)._tag).toBe("Drop")
    })

    it("stops the thread's workers on the user's 'stop', with or without a mention", () => {
      expect(route(inChannel("stop", "U-TRUSTED", "thread_reply"), working)._tag).toBe("StopWorkers")
      expect(route(inChannel("stop", "U-OTHER", "thread_reply"), working)._tag).toBe("Drop")
    })
  })

  it("outside watched channels, takes only the user's mentions", () => {
    const elsewhere = { ...ctx(), threadKnown: false }
    expect(route(inChannel("fix this", "U-TRUSTED", "mention"), elsewhere)._tag).toBe("ToRouter")
    expect(route(inChannel("the deploy is broken"), elsewhere)).toEqual({
      _tag: "Drop",
      reason: "not_for_atta",
    })
    expect(route(inChannel("the deploy is broken", "U-OTHER"), elsewhere)._tag).toBe("Drop")
    expect(route(inChannel("use pnpm", "U-TRUSTED", "thread_reply"), ctx(w("w1", "running")))._tag).toBe(
      "Drop",
    )
  })

  it("acts on the trusted user's decision only", () => {
    expect(route(decision("U-TRUSTED"), ctx())).toEqual({ _tag: "Decide" })
    expect(route(decision("U-OTHER"), ctx())._tag).toBe("Drop")
  })
})
