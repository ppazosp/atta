import { Schema } from "effect"

// Branded so a worker ID can never be passed where an event ID is expected.

export const EventId = Schema.NonEmptyString.pipe(Schema.brand("EventId"))
export type EventId = typeof EventId.Type

export const WorkerId = Schema.NonEmptyString.pipe(Schema.brand("WorkerId"))
export type WorkerId = typeof WorkerId.Type

/** Identifies one run of a worker in the runtime. A follow-up starts a new run on the same worker. */
export const RunId = Schema.NonEmptyString.pipe(Schema.brand("RunId"))
export type RunId = typeof RunId.Type

/** The runtime's own handle for a worker (a T3 thread ID in v0). */
export const RuntimeRef = Schema.NonEmptyString.pipe(Schema.brand("RuntimeRef"))
export type RuntimeRef = typeof RuntimeRef.Type

/**
 * Stable ID of one action atta takes outside (a Slack post, a worker launch). The outbox stores it before the
 * action runs, and the action passes it on, so a retry after a crash can be recognised.
 */
export const OperationId = Schema.NonEmptyString.pipe(Schema.brand("OperationId"))
export type OperationId = typeof OperationId.Type

/** Milliseconds since the Unix epoch. */
export const Timestamp = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)).pipe(Schema.brand("Timestamp"))
export type Timestamp = typeof Timestamp.Type
