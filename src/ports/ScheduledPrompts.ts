import type { Stream } from "effect"
import { Context } from "effect"
import type { Event } from "../core/domain/event.ts"

/** Scheduled prompts from the config file, as events. Each slot fires once, also across restarts. */
export class ScheduledPrompts extends Context.Service<
  ScheduledPrompts,
  {
    readonly events: Stream.Stream<Extract<Event, { _tag: "ScheduledPrompt" }>>
  }
>()("atta/ports/ScheduledPrompts") {}
