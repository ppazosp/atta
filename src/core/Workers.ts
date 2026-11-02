import { Context, Effect, Layer, Option, Ref, Result, Schema, Semaphore } from "effect"
import { EventId, OperationId, WorkerId } from "./domain/ids.ts"
import { transition, type WorkerInput } from "./domain/lifecycle.ts"
import type { ThreadKey } from "./domain/thread.ts"
import type { Brief, RunOutcome, Worker } from "./domain/worker.ts"
import type { PlanTask } from "../ports/Channel.ts"
import type { RunStatus } from "../ports/Runtime.ts"
import { Store, StoreError, type OutboxEntry } from "../ports/Store.ts"
import { now } from "./clock.ts"
import { Intake } from "./Intake.ts"
import { decodePayload, Outbox, Results, type Handlers, type OutboxFailure } from "./Outbox.ts"

export class WorkerUnavailable extends Schema.TaggedError<WorkerUnavailable>()("WorkerUnavailable", {
  workerId: Schema.String,
  state: Schema.String,
  message: Schema.String,
}) {}

/** Appended to every brief. The rules here come from the proofs (a worker followed "use main" and committed there). */
const briefFooter = (workerId: WorkerId) =>
  [
    "",
    "---",
    `You are atta worker ${workerId}. atta's MCP server ("atta") is available to you.`,
    `- When you need context you do not have, call atta.ask with worker_id "${workerId}".`,
    `- When you finish, or if you are blocked, call atta.report with worker_id "${workerId}" and a short result.`,
    "- Work on the branch created for you. Do not switch branches and do not commit to main.",
  ].join("\n")

const launchOp = (workerId: WorkerId) => OperationId.make(`launch:${workerId}`)
const finishOp = (workerId: WorkerId, runId: string) => OperationId.make(`finish:${workerId}:${runId}`)

type Failure = OutboxFailure | WorkerUnavailable

/** What a run ending looks like to core: the runtime's status and the plan as the tracker last drew it. */
export interface RunEnd {
  readonly status: RunStatus
  readonly tasks: ReadonlyArray<PlanTask>
  readonly position: number
}

/**
 * Starts, messages, stops and finishes workers, keeping their state on the statechart.
 *
 * Every operation on one worker runs under that worker's lock and rereads the worker inside its transaction, so
 * a message, a stop and the tracker never act on a stale state. Results of outside actions are recorded in the
 * transaction that marks their outbox entry done, so a crash cannot separate the two.
 */
export class Workers extends Context.Service<
  Workers,
  {
    /** Runs `effect` while holding the worker's lock. The operations below take it themselves. */
    locked<A, E, R>(workerId: WorkerId, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R>
    /**
     * Records a new worker and launches it. `seed` makes it idempotent: the same seed (a router tool call ID)
     * returns the same worker, so a replayed call never starts a second one. A failed launch leaves it `failed`.
     */
    start(input: {
      readonly brief: Brief
      readonly origin: ThreadKey
      readonly seed: string
    }): Effect.Effect<Worker, Failure>
    /**
     * Sends a message: a steer while running, a new run while idle, a continue while interrupted, a retry when
     * failed. Anything else is refused.
     */
    message(input: {
      readonly workerId: WorkerId
      readonly text: string
      readonly operationId: OperationId
    }): Effect.Effect<Worker, Failure>
    stop(input: {
      readonly workerId: WorkerId
      readonly operationId: OperationId
    }): Effect.Effect<Worker, Failure>
    /**
     * Ends the current run in one transaction: the worker moves on the statechart, the plan's last edit is queued
     * and a `WorkerFinished` event is stored for the router. Returns the event ID. Call while holding the lock.
     */
    finishRun(workerId: WorkerId, end: RunEnd): Effect.Effect<EventId, StoreError | WorkerUnavailable>
    /** Retries a launch that has not landed (the tracker calls it for workers still launching). */
    resumeLaunch(workerId: WorkerId): Effect.Effect<Worker, Failure>
    /** Handlers for an outbox entry found pending at startup, so its result lands where it belongs. */
    recoveryHandlers(entry: OutboxEntry): Handlers<StoreError | WorkerUnavailable>
    /** Retires an idle worker whose last change is older than `cutoff`. Does nothing otherwise. */
    expireIdle(workerId: WorkerId, cutoff: number): Effect.Effect<void, StoreError | WorkerUnavailable>
    /** Records changes to a worker that are not state changes (plan message, read position). */
    update(workerId: WorkerId, patch: Partial<Worker>): Effect.Effect<Worker, StoreError | WorkerUnavailable>
    get(workerId: WorkerId): Effect.Effect<Worker, StoreError | WorkerUnavailable>
  }
>()("atta/core/Workers") {
  static readonly layer = Layer.effect(
    Workers,
    Effect.gen(function* () {
      const store = yield* Store
      const outbox = yield* Outbox
      const intake = yield* Intake
      const locks = yield* Ref.make<ReadonlyMap<string, Semaphore.Semaphore>>(new Map())

      const lockFor = (workerId: WorkerId) =>
        Ref.modify(locks, (all): readonly [Semaphore.Semaphore, ReadonlyMap<string, Semaphore.Semaphore>] => {
          const existing = all.get(workerId)
          if (existing) return [existing, all]
          const created = Semaphore.makeUnsafe(1)
          return [created, new Map(all).set(workerId, created)]
        })
      const locked = <A, E, R>(workerId: WorkerId, effect: Effect.Effect<A, E, R>) =>
        lockFor(workerId).pipe(Effect.flatMap((lock) => lock.withPermits(1)(effect)))

      const get = Effect.fn("Workers.get")(function* (workerId: WorkerId) {
        const found = yield* store.workers.get(workerId)
        if (Option.isNone(found)) {
          return yield* new WorkerUnavailable({
            workerId,
            state: "unknown",
            message: `no worker ${workerId}`,
          })
        }
        return found.value
      })

      /** Rereads the worker and applies `input` to its current state, in the caller's transaction. */
      const move = Effect.fn("Workers.move")(function* (
        workerId: WorkerId,
        input: WorkerInput,
        patch: Partial<Worker> = {},
        reason?: string,
      ) {
        const worker = yield* get(workerId)
        const to = transition(worker.state, input)
        if (Result.isFailure(to)) {
          return yield* new WorkerUnavailable({
            workerId,
            state: worker.state,
            message: `cannot ${input} a worker that is ${worker.state}`,
          })
        }
        const at = yield* now
        const next: Worker = { ...worker, ...patch, state: to.success, updatedAt: at }
        yield* store.transitions.append({
          workerId,
          from: worker.state,
          to: to.success,
          input,
          reason: reason ?? null,
          at,
        })
        yield* store.workers.update(next)
        if (to.success === "retired") {
          for (const thread of yield* store.subscriptions.threadsOf(workerId)) {
            yield* store.subscriptions.unsubscribe(thread, workerId)
          }
        }
        return next
      })

      const update = Effect.fn("Workers.update")(function* (workerId: WorkerId, patch: Partial<Worker>) {
        const worker = yield* get(workerId)
        const next: Worker = { ...worker, ...patch }
        yield* store.workers.update(next)
        return next
      })

      const launchHandlers = (workerId: WorkerId): Handlers<StoreError | WorkerUnavailable> => ({
        apply: (raw) =>
          Schema.decodeUnknownEffect(Results.worker_launch)(raw).pipe(
            Effect.mapError((e) => new StoreError({ operation: "worker_launch", message: e.message })),
            Effect.flatMap((launched) =>
              move(workerId, "launched", {
                runtimeRef: launched.runtimeRef,
                runId: launched.runId,
                branch: launched.branch,
                worktreePath: launched.worktreePath,
              }),
            ),
            Effect.asVoid,
          ),
        // A failed launch is final: the router is told, and recovery does not launch it later behind its back.
        onFailure: (error) => move(workerId, "launch_failed", {}, error.message).pipe(Effect.asVoid),
      })

      const sendHandlers = (
        workerId: WorkerId,
        input: WorkerInput,
        mode: "steer" | "queue",
      ): Handlers<StoreError | WorkerUnavailable> => ({
        apply: (raw) =>
          Effect.gen(function* () {
            const sent = yield* Schema.decodeUnknownEffect(Results.worker_send)(raw).pipe(
              Effect.mapError((e) => new StoreError({ operation: "worker_send", message: e.message })),
            )
            const worker = yield* get(workerId)
            // A steer joins the run atta follows; it never changes it. If the runtime turned it into a new run,
            // atta keeps following the old one (a known gap in docs/v0-plan.md).
            if (mode === "steer" && sent.runId !== worker.runId) {
              yield* Effect.logWarning(`steer to ${workerId} went to run ${sent.runId}, not ${worker.runId}`)
            }
            const patch = mode === "queue" ? { runId: sent.runId, planMessage: null } : {}
            // A send replayed at startup may no longer fit (the worker moved on since). Record it, change nothing.
            yield* move(workerId, input, patch).pipe(
              Effect.catchTag("WorkerUnavailable", (error) =>
                Effect.logWarning(`send to ${workerId} no longer applies: ${error.message}`),
              ),
            )
          }),
      })

      const start = Effect.fn("Workers.start")(function* (input: {
        readonly brief: Brief
        readonly origin: ThreadKey
        readonly seed: string
      }) {
        const workerId = WorkerId.make(`w-${input.seed}`)
        return yield* locked(
          workerId,
          Effect.gen(function* () {
            yield* store.transaction(
              Effect.gen(function* () {
                if (Option.isSome(yield* store.workers.get(workerId))) return
                const at = yield* now
                yield* store.workers.insert({
                  id: workerId,
                  brief: input.brief,
                  origin: input.origin,
                  state: "launching",
                  runtimeRef: null,
                  runId: null,
                  branch: null,
                  worktreePath: null,
                  stepPosition: 0,
                  planMessage: null,
                  createdAt: at,
                  updatedAt: at,
                })
                yield* store.subscriptions.subscribe(input.origin, workerId, at)
                yield* outbox.enqueue(launchOp(workerId), "worker_launch", {
                  workerId,
                  repo: input.brief.repo,
                  brief: { ...input.brief, text: input.brief.text + briefFooter(workerId) },
                })
              }),
            )
            return yield* launchLocked(workerId)
          }),
        )
      })

      /** Runs a launch that has not landed yet. A launch the runtime refused leaves the worker failed. */
      const launchLocked = Effect.fn("Workers.launch")(function* (workerId: WorkerId) {
        const worker = yield* get(workerId)
        if (worker.state !== "launching") return worker
        const outcome = yield* outbox.run(launchOp(workerId), launchHandlers(workerId)).pipe(Effect.result)
        const after = yield* get(workerId)
        if (after.state === "launching") {
          const reason = Result.isFailure(outcome) ? outcome.failure.message : "its result was not recorded"
          return yield* new WorkerUnavailable({
            workerId,
            state: after.state,
            message: `the launch has not landed yet (${reason}); atta retries it`,
          })
        }
        return after
      })

      const send = Effect.fn("Workers.send")(function* (
        worker: Worker,
        input: WorkerInput,
        text: string,
        mode: "steer" | "queue",
        operationId: OperationId,
      ) {
        if (worker.runtimeRef === null) {
          return yield* new WorkerUnavailable({
            workerId: worker.id,
            state: worker.state,
            message: "not launched",
          })
        }
        const runtimeRef = worker.runtimeRef
        yield* store.transaction(
          outbox.enqueue(operationId, "worker_send", { workerId: worker.id, worker: runtimeRef, text, mode }),
        )
        yield* outbox.run(operationId, sendHandlers(worker.id, input, mode))
        return yield* get(worker.id)
      })

      const message = Effect.fn("Workers.message")(function* (input: {
        readonly workerId: WorkerId
        readonly text: string
        readonly operationId: OperationId
      }) {
        return yield* locked(
          input.workerId,
          Effect.gen(function* () {
            const worker = yield* get(input.workerId)
            switch (worker.state) {
              case "running":
                return yield* send(worker, "steered", input.text, "steer", input.operationId)
              case "idle":
                return yield* send(worker, "follow_up", input.text, "queue", input.operationId)
              case "interrupted":
                return yield* send(worker, "continue", input.text, "queue", input.operationId)
              case "failed":
                return yield* send(worker, "retry", input.text, "queue", input.operationId)
              default:
                return yield* new WorkerUnavailable({
                  workerId: worker.id,
                  state: worker.state,
                  message: `a ${worker.state} worker cannot take messages`,
                })
            }
          }),
        )
      })

      const stop = Effect.fn("Workers.stop")(function* (input: {
        readonly workerId: WorkerId
        readonly operationId: OperationId
      }) {
        return yield* locked(
          input.workerId,
          Effect.gen(function* () {
            const worker = yield* get(input.workerId)
            if (worker.state !== "running" || worker.runtimeRef === null) {
              return yield* new WorkerUnavailable({
                workerId: worker.id,
                state: worker.state,
                message: "only a running worker can be stopped",
              })
            }
            const runtimeRef = worker.runtimeRef
            const stopping = yield* store.transaction(
              Effect.gen(function* () {
                yield* outbox.enqueue(input.operationId, "worker_stop", {
                  workerId: worker.id,
                  worker: runtimeRef,
                })
                return yield* move(worker.id, "stop_requested")
              }),
            )
            yield* outbox.run(input.operationId)
            return stopping
          }),
        )
      })

      const finishRun = Effect.fn("Workers.finishRun")(function* (workerId: WorkerId, end: RunEnd) {
        const { status } = end
        const wrongBranch =
          status.assignedBranch !== null &&
          status.currentBranch !== null &&
          status.assignedBranch !== status.currentBranch
        // The plan shows the outcome and the steps; the router tells the result below it. Only a worker that
        // committed outside its own branch gets a line in the plan itself.
        const answer = wrongBranch
          ? `Warning: committed on ${status.currentBranch}, not on its own branch ${status.assignedBranch}.`
          : ""
        const eventId = EventId.make(`finished:${workerId}:${status.runId}`)
        yield* store.transaction(
          Effect.gen(function* () {
            const worker = yield* get(workerId)
            // A stopping worker is stopped however its run ended: the run may have ended before the stop landed.
            const outcome: RunOutcome = worker.state === "stopping" ? "stopped" : (status.outcome ?? "failed")
            const input: WorkerInput =
              worker.state === "stopping"
                ? "stopped"
                : outcome === "completed"
                  ? "run_completed"
                  : outcome === "failed"
                    ? "run_failed"
                    : "run_interrupted"
            if (worker.planMessage !== null) {
              yield* outbox.enqueue(finishOp(workerId, status.runId), "plan_finish", {
                plan: worker.planMessage,
                title: worker.brief.title,
                tasks: end.tasks,
                outcome,
                answer: answer === "" ? null : answer,
              })
            }
            yield* move(workerId, input, { stepPosition: end.position })
            yield* intake.accept({
              _tag: "WorkerFinished",
              id: eventId,
              source: "worker",
              externalId: `${workerId}:${status.runId}`,
              receivedAt: yield* now,
              workerId,
              runId: status.runId,
              outcome,
              finalMessage: status.finalMessage,
              branch: status.currentBranch ?? status.assignedBranch,
              commit: status.headCommit,
              wrongBranch,
            })
          }),
        )
        const worker = yield* get(workerId)
        if (worker.planMessage !== null) {
          yield* outbox
            .run(finishOp(workerId, status.runId))
            .pipe(
              Effect.catch((error) => Effect.logWarning(`could not finish the plan of ${workerId}`, error)),
            )
        }
        return eventId
      })

      const recoveryHandlers = (entry: OutboxEntry): Handlers<StoreError | WorkerUnavailable> => {
        switch (entry.kind) {
          case "worker_launch":
            return {
              apply: (raw) =>
                decodePayload("worker_launch", entry.payload).pipe(
                  Effect.flatMap((p) => launchHandlers(p.workerId).apply!(raw)),
                ),
              onFailure: (error) =>
                decodePayload("worker_launch", entry.payload).pipe(
                  Effect.flatMap((p) => launchHandlers(p.workerId).onFailure!(error)),
                ),
            }
          case "worker_send":
            return {
              apply: (raw) =>
                decodePayload("worker_send", entry.payload).pipe(
                  Effect.flatMap((p) =>
                    get(p.workerId).pipe(
                      Effect.flatMap((worker) => {
                        // The worker's state still tells which message this was.
                        const input: WorkerInput =
                          p.mode === "steer"
                            ? "steered"
                            : worker.state === "interrupted"
                              ? "continue"
                              : worker.state === "failed"
                                ? "retry"
                                : "follow_up"
                        return sendHandlers(p.workerId, input, p.mode).apply!(raw)
                      }),
                    ),
                  ),
                ),
            }
          default:
            return {}
        }
      }

      const expireIdle = (workerId: WorkerId, cutoff: number) =>
        locked(
          workerId,
          store.transaction(
            Effect.gen(function* () {
              const worker = yield* get(workerId)
              if (worker.state === "idle" && worker.updatedAt < cutoff) yield* move(workerId, "idle_expired")
            }),
          ),
        )

      return Workers.of({
        locked,
        start,
        message,
        stop,
        finishRun,
        expireIdle,
        resumeLaunch: (workerId) => locked(workerId, launchLocked(workerId)),
        recoveryHandlers,
        update,
        get,
      })
    }),
  )
}
