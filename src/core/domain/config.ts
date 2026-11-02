import { Schema } from "effect"
import { Conversation, ThreadRef } from "./thread.ts"

/** Where a scheduled prompt goes: an existing thread, or a new thread in a conversation. */
export const ScheduleTarget = Schema.Union([
  Schema.Struct({ thread: ThreadRef }),
  Schema.Struct({ conversation: Conversation }),
])
export type ScheduleTarget = typeof ScheduleTarget.Type

const ReasoningEffort = Schema.Literals(["low", "medium", "high", "xhigh"])

export const RepoConfig = Schema.Struct({
  /** The name the router uses, for example `sandbox-repo`. */
  name: Schema.NonEmptyString,
  path: Schema.NonEmptyString,
  /** The T3 project that points at `path`. */
  t3ProjectId: Schema.NonEmptyString,
  /** The branch workers start their worktrees from. Defaults to main. */
  baseBranch: Schema.optionalKey(Schema.NonEmptyString),
})
export type RepoConfig = typeof RepoConfig.Type

export const ScheduleConfig = Schema.Struct({
  name: Schema.NonEmptyString,
  /** Five-field cron expression, in `timezone`. */
  cron: Schema.NonEmptyString,
  timezone: Schema.NonEmptyString,
  target: ScheduleTarget,
  text: Schema.NonEmptyString,
})
export type ScheduleConfig = typeof ScheduleConfig.Type

/** Everything in `atta.config.ts`. Secrets are not here: tokens come from the environment or the Keychain. */
export const AttaConfig = Schema.Struct({
  slack: Schema.Struct({
    /** The only Slack user whose messages atta acts on in v0. */
    trustedUserId: Schema.NonEmptyString,
  }),
  repos: Schema.Array(RepoConfig),
  router: Schema.Struct({
    model: Schema.NonEmptyString,
    effort: ReasoningEffort,
  }),
  workers: Schema.Struct({
    model: Schema.NonEmptyString,
    effort: ReasoningEffort,
    /** T3 provider instance for workers; its launch settings add atta's MCP server. */
    t3InstanceId: Schema.NonEmptyString,
    /** Days an idle worker stays subscribed to its thread. */
    idleDays: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
  }),
  t3: Schema.Struct({
    url: Schema.NonEmptyString,
  }),
  mcp: Schema.Struct({
    port: Schema.Int.check(Schema.isBetween({ minimum: 1024, maximum: 65535 })),
  }),
  schedules: Schema.Array(ScheduleConfig),
  /** Where atta keeps its database, router storage and logs. */
  dataDir: Schema.NonEmptyString,
})
export type AttaConfig = typeof AttaConfig.Type
