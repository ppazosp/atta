import { Schema } from "effect"
import { Event } from "../../core/domain/event.ts"
import { EventId, OperationId, Timestamp, WorkerId } from "../../core/domain/ids.ts"
import { WorkerInput } from "../../core/domain/lifecycle.ts"
import { MessageRef } from "../../core/domain/thread.ts"
import { Brief, Worker, WorkerState } from "../../core/domain/worker.ts"

export const EventRow = Schema.Struct({ data: Schema.fromJsonString(Event) })

export const WorkerRow = Schema.Struct({
  ...Worker.fields,
  brief: Schema.fromJsonString(Brief),
  planMessage: Schema.NullOr(Schema.fromJsonString(MessageRef)),
})

export const TransitionRow = Schema.Struct({
  workerId: WorkerId,
  from: WorkerState,
  to: WorkerState,
  input: WorkerInput,
  reason: Schema.NullOr(Schema.String),
  at: Timestamp,
})

export const SubscriberRow = Schema.Struct({ workerId: WorkerId, state: WorkerState })

export const DeliveryRow = Schema.Struct({
  eventId: EventId,
  target: Schema.Union([
    Schema.Literals(["router", "observe", "core"]),
    Schema.TemplateLiteral(["worker:", Schema.String]),
  ]),
  status: Schema.Literals(["pending", "done"]),
  attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  createdAt: Timestamp,
})

export const Json = Schema.fromJsonString(Schema.Json)

export const OutboxRow = Schema.Struct({
  operationId: OperationId,
  kind: Schema.Literals([
    "slack_reply",
    "slack_react",
    "slack_unreact",
    "plan_upsert",
    "plan_finish",
    "worker_launch",
    "worker_send",
    "worker_stop",
    "approval_request",
    "approval_close",
    "ask_post",
    "ask_close",
  ]),
  payload: Json,
  status: Schema.Literals(["pending", "done", "failed"]),
  result: Json,
  createdAt: Timestamp,
  updatedAt: Timestamp,
})
