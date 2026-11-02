import { Context, Effect, Layer, Queue, Stream } from "effect"
import type { Event } from "../../src/core/domain/event.ts"
import { ScheduledPrompts } from "../../src/ports/ScheduledPrompts.ts"

type ScheduledPrompt = Extract<Event, { _tag: "ScheduledPrompt" }>

export class FakeScheduledPromptsControl extends Context.Service<
  FakeScheduledPromptsControl,
  { fire(event: ScheduledPrompt): Effect.Effect<void> }
>()("atta/test/FakeScheduledPromptsControl") {}

export const FakeScheduledPromptsLayer = Layer.effectContext(
  Effect.gen(function* () {
    const queue = yield* Queue.unbounded<ScheduledPrompt>()
    return Context.make(ScheduledPrompts, ScheduledPrompts.of({ events: Stream.fromQueue(queue) })).pipe(
      Context.add(
        FakeScheduledPromptsControl,
        FakeScheduledPromptsControl.of({ fire: (event) => Queue.offer(queue, event).pipe(Effect.asVoid) }),
      ),
    )
  }),
)
