import { Context } from "effect"
import type { AttaConfig } from "./domain/config.ts"

/** The decoded config file. The app reads it (WP6) and provides it to core. */
export class Settings extends Context.Service<Settings, AttaConfig>()("atta/core/Settings") {}

/** How often the tracker reads running workers, in milliseconds. */
export const TRACKER_INTERVAL_MS = 2_000
