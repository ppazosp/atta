import { Clock, Effect } from "effect"
import { Timestamp } from "./domain/ids.ts"

export const now = Clock.currentTimeMillis.pipe(Effect.map((ms) => Timestamp.make(ms)))
