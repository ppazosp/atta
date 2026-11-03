import { Context } from "effect"
import type { AttaConfig } from "./domain/config.ts"

/** The decoded config file. The app reads it (WP6) and provides it to core. */
export class Settings extends Context.Service<Settings, AttaConfig>()("atta/core/Settings") {}

/**
 * Each configured repository's AGENTS.md, by repository name, read when atta starts. The router gets them so it
 * knows what each repository is; workers read them from their worktrees themselves.
 */
export class RepoGuides extends Context.Service<RepoGuides, ReadonlyMap<string, string>>()(
  "atta/core/RepoGuides",
) {}

/** How often the tracker reads running workers, in milliseconds. */
export const TRACKER_INTERVAL_MS = 2_000
