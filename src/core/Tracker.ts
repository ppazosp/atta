import { Context, Effect, Layer, Ref, Schema } from "effect"
import { EventId, OperationId, type WorkerId } from "./domain/ids.ts"
import type { WorkerInput } from "./domain/lifecycle.ts"
import { parseThreadKey } from "./domain/thread.ts"
import type { RunOutcome, Step, Worker } from "./domain/worker.ts"
import type { PlanTask } from "../ports/Channel.ts"
import { Channel } from "../ports/Channel.ts"
import { Runtime } from "../ports/Runtime.ts"
import { Store } from "../ports/Store.ts"
import { now } from "./clock.ts"
import { Dispatch } from "./Dispatch.ts"
import { Intake } from "./Intake.ts"
import { Outbox, Results } from "./Outbox.ts"
import { Settings } from "./Settings.ts"
import { Workers } from "./Workers.ts"

/** Steps shown as plan tasks. Reasoning, checkpoints and the worker's own messages would only add noise. */
const shown = new Set<Step["kind"]>(["command", "file_change", "other"])

interface RunView {
  readonly runId: string
  readonly position: number
  readonly tasks: ReadonlyArray<PlanTask>
}

const DAY_MS = 24 * 60 * 60 * 1000

/**
 * Follows running workers: reads their steps into the plan message, and when a run ends, finishes the plan, moves
 * the worker on the statechart and tells the router with a `WorkerFinished` event.
 */
export class Tracker extends Context.Service<
  Tracker,
  {
    /** One pass over every running or stopping worker, plus retiring idle workers past their time. */
    readonly tick: Effect.Effect<void>
    poll(workerId: WorkerId): Effect.Effect<void>
  }
>()("atta/core/Tracker") {
  static readonly layer = Layer.effect(
    Tracker,
    Effect.gen(function* () {
      const store = yield* Store
      const runtime = yield* Runtime
      const channel = yield* Channel
      const outbox = yield* Outbox
      const workers = yield* Workers
      const intake = yield* Intake
      const dispatch = yield* Dispatch
      const settings = yield* Settings
      // What each worker's plan shows. Lost on restart, then rebuilt by reading the run from the start.
      const views = yield* Ref.make<ReadonlyMap<string, RunView>>(new Map())

      const readSteps = Effect.fn("Tracker.readSteps")(function* (worker: Worker, view: RunView) {
        let { position, tasks } = view
        let changed = false
        for (;;) {
          const page = yield* runtime.steps({ worker: worker.runtimeRef!, afterPosition: position })
          for (const step of page.steps) {
            if (step.runId !== worker.runId || !shown.has(step.kind)) continue
            const task: PlanTask = {
              id: step.id,
              title: step.title,
              status: step.status,
              detail: step.detail,
            }
            const index = tasks.findIndex((t) => t.id === step.id)
            tasks = index === -1 ? [...tasks, task] : tasks.map((t, i) => (i === index ? task : t))
            changed = true
          }
          if (page.nextPosition === position) break
          position = page.nextPosition
        }
        return { view: { runId: view.runId, position, tasks }, changed }
      })

      const finish = Effect.fn("Tracker.finish")(function* (worker: Worker, view: RunView) {
        const status = yield* runtime.status({ worker: worker.runtimeRef!, runId: worker.runId! })
        if (status.state !== "finished") return
        const outcome: RunOutcome = status.outcome ?? "failed"
        const wrongBranch =
          status.assignedBranch !== null &&
          status.currentBranch !== null &&
          status.assignedBranch !== status.currentBranch
        const input: WorkerInput =
          outcome === "completed"
            ? "run_completed"
            : outcome === "failed"
              ? "run_failed"
              : outcome === "stopped" && worker.state === "stopping"
                ? "stopped"
                : "run_interrupted"
        const answer = [
          ...(wrongBranch
            ? [
                `Warning: committed on ${status.currentBranch}, not on its own branch ${status.assignedBranch}.`,
              ]
            : []),
          ...(status.finalMessage ? [status.finalMessage] : []),
        ].join("\n\n")
        const finishOp = OperationId.make(`finish:${worker.id}:${worker.runId}`)
        const eventId = EventId.make(`finished:${worker.id}:${worker.runId}`)
        yield* store.transaction(
          Effect.gen(function* () {
            if (worker.planMessage !== null) {
              yield* outbox.enqueue(finishOp, "plan_finish", {
                plan: worker.planMessage,
                title: worker.brief.title,
                tasks: view.tasks,
                outcome,
                answer: answer === "" ? null : answer,
              })
            }
            yield* workers.move(worker, input, { stepPosition: view.position })
          }),
        )
        if (worker.planMessage !== null) yield* outbox.run(finishOp)
        const routed = yield* intake.accept({
          _tag: "WorkerFinished",
          id: eventId,
          source: "worker",
          externalId: `${worker.id}:${worker.runId}`,
          receivedAt: yield* now,
          workerId: worker.id,
          runId: worker.runId!,
          outcome,
          finalMessage: status.finalMessage,
          branch: status.currentBranch ?? status.assignedBranch,
          commit: status.headCommit,
          wrongBranch,
        })
        if (routed !== null) yield* dispatch.deliver(eventId)
        yield* Ref.update(views, (all) => {
          const next = new Map(all)
          next.delete(worker.id)
          return next
        })
      })

      const poll = Effect.fn("Tracker.poll")(function* (workerId: WorkerId) {
        let worker = yield* workers.get(workerId)
        if (worker.runtimeRef === null || worker.runId === null) return
        if (worker.state !== "running" && worker.state !== "stopping") return
        const runId = worker.runId

        if (worker.planMessage === null) {
          const planOp = OperationId.make(`plan:${worker.id}:${worker.runId}`)
          yield* store.transaction(
            outbox.enqueue(planOp, "plan_upsert", {
              thread: parseThreadKey(worker.origin),
              title: worker.brief.title,
            }),
          )
          const plan = yield* outbox
            .run(planOp)
            .pipe(Effect.flatMap(Schema.decodeUnknownEffect(Results.plan_upsert)))
          worker = { ...worker, planMessage: plan }
          yield* store.workers.update(worker)
        }

        const known = (yield* Ref.get(views)).get(worker.id)
        // After a restart, or for a new run, rebuild the plan from the start of the thread's steps.
        const base: RunView =
          known !== undefined && known.runId === worker.runId ? known : { runId, position: 0, tasks: [] }
        const { view, changed } = yield* readSteps(worker, base)
        yield* Ref.update(views, (all) => new Map(all).set(worker.id, view))
        if (changed) {
          yield* channel
            .upsertPlan({
              thread: parseThreadKey(worker.origin),
              plan: worker.planMessage,
              title: worker.brief.title,
              tasks: view.tasks,
              operationId: OperationId.make(`plan:${worker.id}:${worker.runId}`),
            })
            .pipe(Effect.catch((error) => Effect.logWarning(`plan update for ${worker.id} failed`, error)))
        }
        if (view.position !== worker.stepPosition) {
          worker = { ...worker, stepPosition: view.position }
          yield* store.workers.update(worker)
        }
        yield* finish(worker, view)
      })

      const retireIdle = Effect.fn("Tracker.retireIdle")(function* () {
        const cutoff = (yield* now) - settings.workers.idleDays * DAY_MS
        for (const worker of yield* store.workers.inStates(["idle"])) {
          if (worker.updatedAt < cutoff) yield* store.transaction(workers.move(worker, "idle_expired"))
        }
      })

      const tick = Effect.gen(function* () {
        for (const worker of yield* store.workers.inStates(["running", "stopping"])) {
          yield* poll(worker.id).pipe(
            Effect.catch((error) => Effect.logWarning(`tracking ${worker.id} failed`, error)),
          )
        }
        yield* retireIdle().pipe(
          Effect.catch((error) => Effect.logWarning("retiring idle workers failed", error)),
        )
      }).pipe(Effect.catch((error) => Effect.logError("tracker pass failed", error)))

      return Tracker.of({
        tick,
        poll: (workerId) =>
          poll(workerId).pipe(
            Effect.catch((error) => Effect.logWarning(`tracking ${workerId} failed`, error)),
          ),
      })
    }),
  )
}
