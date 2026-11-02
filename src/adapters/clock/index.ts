import { Cron, DateTime, Effect, Layer, Result, Schema, Stream } from "effect"
import { Settings } from "../../core/Settings.ts"
import type { ScheduleConfig } from "../../core/domain/config.ts"
import type { Event } from "../../core/domain/event.ts"
import { EventId, Timestamp } from "../../core/domain/ids.ts"
import { ScheduledPrompts } from "../../ports/ScheduledPrompts.ts"

export class ClockConfigError extends Schema.TaggedError<ClockConfigError>()("ClockConfigError", {
  message: Schema.String,
}) {}

const scheduledEvent = Effect.fn("Clock.scheduledEvent")(function* (
  schedule: ScheduleConfig,
  slot: number,
): Effect.fn.Return<Extract<Event, { _tag: "ScheduledPrompt" }>> {
  const now = yield* DateTime.now
  const delay = slot - DateTime.toEpochMillis(now)
  if (delay > 0) yield* Effect.sleep(delay)
  const receivedAt = yield* DateTime.now
  const externalId = `${schedule.name}@${DateTime.formatIso(DateTime.makeUnsafe(slot))}`
  return {
    _tag: "ScheduledPrompt",
    id: EventId.make(`clock:${externalId}`),
    source: "clock",
    externalId,
    receivedAt: Timestamp.make(DateTime.toEpochMillis(receivedAt)),
    schedule: schedule.name,
    slot: Timestamp.make(slot),
    target: schedule.target,
    text: schedule.text,
  }
})

export const ClockLayer = Layer.effect(
  ScheduledPrompts,
  Effect.gen(function* () {
    const settings = yield* Settings
    const startedAt = DateTime.toEpochMillis(yield* DateTime.now)
    const names = new Set<string>()
    const streams = yield* Effect.forEach(
      settings.schedules,
      Effect.fn("Clock.schedule")(function* (schedule, index) {
        if (names.has(schedule.name)) {
          return yield* new ClockConfigError({
            message: `Invalid schedules[${index}].name: duplicate schedule name ${schedule.name}`,
          })
        }
        names.add(schedule.name)
        const parsed = Cron.parse(schedule.cron, schedule.timezone)
        if (Result.isFailure(parsed)) {
          return yield* new ClockConfigError({
            message: `Invalid schedules[${index}].cron or schedules[${index}].timezone (${schedule.name}): ${parsed.failure.message}`,
          })
        }
        const cron = parsed.success
        const firstSlot = yield* Effect.try({
          try: () => {
            // Cron.prev is strict; the extra millisecond includes a slot due exactly at startup.
            const latest = Cron.prev(cron, startedAt + 1).getTime()
            const next = Cron.next(cron, startedAt).getTime()
            return startedAt - latest <= 3_600_000 ? latest : next
          },
          catch: () =>
            new ClockConfigError({
              message: `Invalid schedules[${index}].cron (${schedule.name}): no matching slot`,
            }),
        })
        return Stream.iterate(firstSlot, (slot) => Cron.next(cron, slot).getTime()).pipe(
          Stream.mapEffect((slot) => scheduledEvent(schedule, slot)),
        )
      }),
    )
    return ScheduledPrompts.of({ events: Stream.mergeAll(streams, { concurrency: "unbounded" }) })
  }),
)
