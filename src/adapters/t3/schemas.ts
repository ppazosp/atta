import { Schema } from "effect"
import { RunId, RuntimeRef } from "../../core/domain/ids.ts"

const Position = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))

export const Config = Schema.Struct({
  url: Schema.NonEmptyString,
  tokenDirectory: Schema.NonEmptyString,
  repos: Schema.Array(
    Schema.Struct({
      name: Schema.NonEmptyString,
      t3ProjectId: Schema.NonEmptyString,
      baseBranch: Schema.NonEmptyString,
    }),
  ),
  instanceId: Schema.NonEmptyString,
  model: Schema.NonEmptyString,
  effort: Schema.Literals(["low", "medium", "high", "xhigh"]),
})

export const ToolResult = Schema.Struct({
  isError: Schema.optionalKey(Schema.Boolean),
  structuredContent: Schema.optionalKey(Schema.Unknown),
  content: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        type: Schema.String,
        text: Schema.optionalKey(Schema.String),
      }),
    ),
  ),
})

export const LaunchResult = Schema.Struct({
  threadId: RuntimeRef,
  runId: RunId,
  branch: Schema.optionalKey(Schema.NullOr(Schema.String)),
  worktreePath: Schema.optionalKey(Schema.NullOr(Schema.String)),
})

export const SendResult = Schema.Struct({ runId: RunId })
export const StopResult = Schema.Struct({
  threadId: RuntimeRef,
  runId: Schema.NullOr(RunId),
  status: Schema.Literals([
    "interrupt_requested",
    "no_active_run",
    "completed",
    "failed",
    "cancelled",
    "interrupted",
    "rolled_back",
  ]),
})

const ItemStatus = Schema.Literals([
  "idle",
  "pending",
  "queued",
  "preparing",
  "starting",
  "running",
  "waiting",
  "in_progress",
  "completed",
  "interrupted",
  "cancelled",
  "failed",
  "error",
])

export const Item = Schema.Struct({
  itemId: Schema.NonEmptyString,
  position: Position,
  runId: Schema.NullOr(RunId),
  type: Schema.NonEmptyString,
  status: ItemStatus,
  title: Schema.NullOr(Schema.String),
  text: Schema.NullOr(Schema.String),
  textTruncated: Schema.Boolean,
  nextTextOffset: Schema.optionalKey(Schema.NullOr(Position)),
})
export type Item = typeof Item.Type

const Page = {
  items: Schema.Array(Item),
  nextPosition: Schema.NullOr(Position),
  hasMore: Schema.Boolean,
}

export const ActivityPage = Schema.Struct(Page)
export const StatusPage = Schema.Struct({
  ...Page,
  thread: Schema.Struct({
    threadId: RuntimeRef,
    branch: Schema.NullOr(Schema.String),
    worktreePath: Schema.NullOr(Schema.String),
  }),
  recentRuns: Schema.Array(Schema.Struct({ runId: RunId, status: ItemStatus })),
})
