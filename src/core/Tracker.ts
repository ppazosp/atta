import { Context, Effect, Layer, Ref, Schema } from "effect"
import { OperationId, type RunId, type WorkerId } from "./domain/ids.ts"
import { parseThreadKey } from "./domain/thread.ts"
import type { Step, Worker } from "./domain/worker.ts"
import type { PlanTask } from "../ports/Channel.ts"
import { Channel } from "../ports/Channel.ts"
import { Runtime } from "../ports/Runtime.ts"
import { Store } from "../ports/Store.ts"
import { now } from "./clock.ts"
import { Dispatch } from "./Dispatch.ts"
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
 * Follows running workers: reads their steps into the plan message, and when a run ends, has the worker manager
 * finish it (state, plan, event in one transaction) and delivers the event to the router.
 */
export class Tracker extends Context.Service<
  Tracker,
  {
    /** One pass: every running or stopping worker, idle workers past their time, and deliveries left pending. */
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
      const dispatch = yield* Dispatch
      const settings = yield* Settings
      // What each worker's plan shows. Lost on restart, then rebuilt by reading the run from the start.
      const views = yield* Ref.make<ReadonlyMap<string, RunView>>(new Map())
      const forget = (workerId: WorkerId) =>
        Ref.update(views, (all) => {
          const next = new Map(all)
          next.delete(workerId)
          return next
        })

      const readSteps = Effect.fn("Tracker.readSteps")(function* (
        worker: Worker,
        runId: RunId,
        view: RunView,
      ) {
        let { position, tasks } = view
        let changed = false
        for (;;) {
          const page = yield* runtime.steps({ worker: worker.runtimeRef!, afterPosition: position })
          for (const step of page.steps) {
            if (step.runId !== runId || !shown.has(step.kind)) continue
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
        return { view: { runId, position, tasks }, changed }
      })

      const pollLocked = Effect.fn("Tracker.poll")(function* (workerId: WorkerId) {
        let worker = yield* workers.get(workerId)
        if (worker.runtimeRef === null || worker.runId === null) return
        if (worker.state !== "running" && worker.state !== "stopping") return
        const runId = worker.runId

        if (worker.planMessage === null) {
          const planOp = OperationId.make(`plan:${worker.id}:${runId}`)
          yield* store.transaction(
            outbox.enqueue(planOp, "plan_upsert", {
              thread: parseThreadKey(worker.origin),
              title: worker.brief.title,
            }),
          )
          const plan = yield* outbox
            .run(planOp)
            .pipe(Effect.flatMap(Schema.decodeUnknownEffect(Results.plan_upsert)))
          worker = yield* workers.update(worker.id, { planMessage: plan })
        }

        const known = (yield* Ref.get(views)).get(worker.id)
        // After a restart, or for a new run, rebuild the plan from the start of the thread's steps.
        const base: RunView =
          known !== undefined && known.runId === runId ? known : { runId, position: 0, tasks: [] }
        const { view, changed } = yield* readSteps(worker, runId, base)
        yield* Ref.update(views, (all) => new Map(all).set(worker.id, view))
        if (changed) {
          yield* channel
            .upsertPlan({
              thread: parseThreadKey(worker.origin),
              plan: worker.planMessage,
              title: worker.brief.title,
              tasks: view.tasks,
              operationId: OperationId.make(`plan:${worker.id}:${runId}`),
            })
            .pipe(Effect.catch((error) => Effect.logWarning(`plan update for ${worker.id} failed`, error)))
        }
        if (view.position !== worker.stepPosition)
          worker = yield* workers.update(worker.id, { stepPosition: view.position })

        const status = yield* runtime.status({ worker: worker.runtimeRef!, runId })
        if (status.state !== "finished") return
        const eventId = yield* workers.finishRun(worker.id, {
          status,
          tasks: view.tasks,
          position: view.position,
        })
        yield* forget(worker.id)
        yield* dispatch.deliver(eventId)
      })

      const poll = (workerId: WorkerId) =>
        workers
          .locked(workerId, pollLocked(workerId))
          .pipe(Effect.catch((error) => Effect.logWarning(`tracking ${workerId} failed`, error)))

      const tick = Effect.gen(function* () {
        for (const worker of yield* store.workers.inStates(["running", "stopping"])) yield* poll(worker.id)
        const cutoff = (yield* now) - settings.workers.idleDays * DAY_MS
        for (const worker of yield* store.workers.inStates(["idle"])) {
          yield* workers
            .expireIdle(worker.id, cutoff)
            .pipe(Effect.catch((error) => Effect.logWarning(`retiring ${worker.id} failed`, error)))
        }
        // Deliveries a crash or a failed step left behind, including finished runs recorded but not yet told.
        yield* dispatch.recover
      }).pipe(Effect.catch((error) => Effect.logError("tracker pass failed", error)))

      return Tracker.of({ tick, poll })
    }),
  )
}
