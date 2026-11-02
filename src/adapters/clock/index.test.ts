import { describe, expect, it } from "@effect/vitest"
import { DateTime, Deferred, Effect, Ref, Schema, Stream } from "effect"
import { TestClock } from "effect/testing"
import example from "../../../atta.config.example.ts"
import { Settings } from "../../core/Settings.ts"
import { AttaConfig, type ScheduleConfig } from "../../core/domain/config.ts"
import { Event } from "../../core/domain/event.ts"
import { ScheduledPrompts } from "../../ports/ScheduledPrompts.ts"
import { ClockConfigError, ClockLayer } from "./index.ts"

type Prompt = Extract<Event, { _tag: "ScheduledPrompt" }>
const settings = Schema.decodeUnknownSync(AttaConfig)(example)
const millis = (iso: string) => DateTime.toEpochMillis(DateTime.makeUnsafe(iso))
const schedule: ScheduleConfig = {
  name: "morning",
  cron: "0 9 * * *",
  timezone: "Europe/Madrid",
  target: { thread: { team: "T-TEST", conversation: "C-TEST", rootTs: "100.001" } },
  text: "What finished yesterday?",
}

const runAt = Effect.fn("ClockTest.runAt")(function* <A, E, R>(
  iso: string,
  schedules: ReadonlyArray<ScheduleConfig>,
  program: Effect.Effect<A, E, R>,
) {
  yield* TestClock.setTime(millis(iso))
  return yield* program.pipe(
    Effect.provide(ClockLayer),
    Effect.provideService(Settings, { ...settings, schedules }),
  )
})

const observe = Effect.fn("ClockTest.observe")(function* (awaitStartup = false) {
  const prompts = yield* ScheduledPrompts
  const received = yield* Ref.make<Array<Prompt>>([])
  const first = yield* Deferred.make<void>()
  yield* prompts.events.pipe(
    Stream.runForEach((event) =>
      Ref.update(received, (events) => [...events, event]).pipe(
        Effect.andThen(Deferred.succeed(first, undefined)),
      ),
    ),
    Effect.forkScoped,
  )
  if (awaitStartup) yield* Deferred.await(first)
  yield* TestClock.adjust(0)
  return received
})

describe("ClockLayer", () => {
  it.effect.each([
    { start: "2026-07-01T06:59:00.000Z", slot: "2026-07-01T07:00:00.000Z" },
    { start: "2026-11-02T07:59:00.000Z", slot: "2026-11-02T08:00:00.000Z" },
  ])("fires once per slot at 09:00 in the configured time zone %#", ({ start, slot }) =>
    runAt(
      start,
      [schedule],
      Effect.gen(function* () {
        const received = yield* observe()
        yield* TestClock.adjust(59_999)
        expect(yield* Ref.get(received)).toEqual([])
        yield* TestClock.adjust(1)
        const first = (yield* Ref.get(received))[0]!
        expect(Schema.is(Event)(first)).toBe(true)
        expect(first).toMatchObject({
          _tag: "ScheduledPrompt",
          source: "clock",
          externalId: `morning@${slot}`,
          schedule: "morning",
          slot: millis(slot),
          receivedAt: millis(slot),
          target: schedule.target,
          text: schedule.text,
        })
        yield* TestClock.adjust("1 day")
        const events = yield* Ref.get(received)
        expect(events).toHaveLength(2)
        expect(events[1]?.slot).toBe(millis(slot) + 86_400_000)
        expect(new Set(events.map((event) => event.externalId)).size).toBe(2)
      }),
    ).pipe(Effect.scoped),
  )

  it.effect("merges simultaneous schedules and preserves a conversation target", () =>
    runAt(
      "2026-11-02T07:59:00.000Z",
      [
        schedule,
        {
          ...schedule,
          name: "other",
          timezone: "UTC",
          cron: "0 8 * * *",
          target: { conversation: { team: "T-TEST", conversation: "D-TEST" } },
        },
      ],
      Effect.gen(function* () {
        const received = yield* observe()
        yield* TestClock.adjust("1 minute")
        const events = yield* Ref.get(received)
        expect(events).toHaveLength(2)
        expect(events.map((event) => event.schedule).toSorted()).toEqual(["morning", "other"])
        expect(events.find((event) => event.schedule === "other")?.target).toEqual({
          conversation: { team: "T-TEST", conversation: "D-TEST" },
        })
        expect(new Set(events.map((event) => event.id)).size).toBe(2)
      }),
    ).pipe(Effect.scoped),
  )

  it.effect.each([
    "2026-11-02T08:00:00.000Z",
    "2026-11-02T08:00:00.001Z",
    "2026-11-02T08:30:00.000Z",
    "2026-11-02T09:00:00.000Z",
  ])("emits a latest slot within the hour once at startup: %s", (start) =>
    runAt(
      start,
      [schedule],
      Effect.gen(function* () {
        const received = yield* observe(true)
        yield* TestClock.adjust("1 minute")
        const events = yield* Ref.get(received)
        expect(events).toHaveLength(1)
        expect(events[0]?.externalId).toBe("morning@2026-11-02T08:00:00.000Z")
        expect(events[0]?.slot).toBe(millis("2026-11-02T08:00:00.000Z"))
        expect(events[0]?.receivedAt).toBe(millis(start))
      }),
    ).pipe(Effect.scoped),
  )

  it.effect("recovers only the latest slot rather than every missed slot", () =>
    runAt(
      "2026-11-02T08:32:00.000Z",
      [{ ...schedule, cron: "*/5 * * * *" }],
      Effect.gen(function* () {
        const received = yield* observe(true)
        expect((yield* Ref.get(received)).map((event) => event.externalId)).toEqual([
          "morning@2026-11-02T08:30:00.000Z",
        ])
        yield* TestClock.adjust("3 minutes")
        expect((yield* Ref.get(received)).map((event) => event.externalId)).toEqual([
          "morning@2026-11-02T08:30:00.000Z",
          "morning@2026-11-02T08:35:00.000Z",
        ])
      }),
    ).pipe(Effect.scoped),
  )

  it.effect("skips a missed slot older than an hour and waits for the next one", () =>
    runAt(
      "2026-11-02T09:00:00.001Z",
      [schedule],
      Effect.gen(function* () {
        const received = yield* observe()
        expect(yield* Ref.get(received)).toEqual([])
        yield* TestClock.adjust(82_799_999)
        const events = yield* Ref.get(received)
        expect(events).toHaveLength(1)
        expect(events[0]?.externalId).toBe("morning@2026-11-03T08:00:00.000Z")
      }),
    ).pipe(Effect.scoped),
  )

  it.effect("replays the same slot identity after a restart for store deduplication", () =>
    Effect.gen(function* () {
      const first = yield* runAt(
        "2026-11-02T08:10:00.000Z",
        [schedule],
        ScheduledPrompts.use((prompts) => prompts.events.pipe(Stream.take(1), Stream.runCollect)),
      )
      const restarted = yield* runAt(
        "2026-11-02T08:20:00.000Z",
        [schedule],
        ScheduledPrompts.use((prompts) => prompts.events.pipe(Stream.take(1), Stream.runCollect)),
      )
      expect(first[0]?.externalId).toBe("morning@2026-11-02T08:00:00.000Z")
      expect(restarted[0]?.externalId).toBe(first[0]?.externalId)
      expect(restarted[0]?.id).toBe(first[0]?.id)
      expect(restarted[0]?.receivedAt).toBe(millis("2026-11-02T08:20:00.000Z"))
    }),
  )

  it.effect("keeps 09:00 local across the daylight saving change", () =>
    runAt(
      "2026-10-24T06:59:00.000Z",
      [schedule],
      Effect.gen(function* () {
        const received = yield* observe()
        yield* TestClock.adjust("1 minute")
        yield* TestClock.adjust("24 hours")
        expect(yield* Ref.get(received)).toHaveLength(1)
        yield* TestClock.adjust("1 hour")
        expect((yield* Ref.get(received)).map((event) => event.externalId)).toEqual([
          "morning@2026-10-24T07:00:00.000Z",
          "morning@2026-10-25T08:00:00.000Z",
        ])
      }),
    ).pipe(Effect.scoped),
  )

  it.effect("has no events when no schedules are configured", () =>
    runAt(
      "2026-11-02T08:00:00.000Z",
      [],
      ScheduledPrompts.use((prompts) =>
        prompts.events.pipe(
          Stream.runCollect,
          Effect.map((events) => expect(events).toEqual([])),
        ),
      ),
    ),
  )

  it.effect.each([
    { schedules: [{ ...schedule, cron: "invalid" }], field: "cron" },
    { schedules: [{ ...schedule, timezone: "Not/A_Zone" }], field: "timezone" },
    { schedules: [{ ...schedule, cron: "0 0 31 2 *" }], field: "cron" },
    { schedules: [schedule, schedule], field: "name" },
  ])("rejects an invalid schedule at layer construction %#", ({ schedules, field }) =>
    Effect.gen(function* () {
      const error = yield* runAt("2026-11-02T08:00:00.000Z", schedules, ScheduledPrompts).pipe(Effect.flip)
      expect(error).toBeInstanceOf(ClockConfigError)
      expect(error.message).toContain("schedules[")
      expect(error.message).toContain(field)
    }),
  )
})
