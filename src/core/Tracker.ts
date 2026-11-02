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

/** Steps shown when the worker has no plan yet. Reasoning, checkpoints and messages would only add noise. */
const shownWithoutPlan = new Set<Step["kind"]>(["command", "file_change", "other"])

const TITLE_LENGTH = 60

/**
 * A short task name for the plan message: the command without its shell wrapper, cut to one short line. The
 * command's text and output stay in T3; Slack only needs to show what the worker is doing.
 */
export const taskTitle = (step: Step): string => {
  const line = (step.title.split("\n")[0] ?? "").trim()
  const command = line
    .replace(/^\$\s*/, "")
    .replace(/^\/bin\/(?:zsh|bash|sh)\s+-l?c\s+/, "")
    .replace(/^(['"])(.*?)\1?$/, "$2")
    .trim()
  const title = command === "" ? step.kind.replace("_", " ") : command
  return title.length > TITLE_LENGTH ? `${title.slice(0, TITLE_LENGTH - 1)}…` : title
}

interface RunView {
  readonly runId: string
  readonly position: number
  /** The worker's own plan steps, as the latest version of its plan lists them. */
  readonly plan: ReadonlyArray<PlanTask>
  /** Other steps, shown only while there is no plan. */
  readonly steps: ReadonlyArray<PlanTask>
}

/**
 * What the plan message shows: the worker's own plan when it has one, otherwise its steps. A failed command is
 * normal work (a test that fails before the fix), so it shows as done; only a failed run gets a warning.
 */
const shownTasks = (view: RunView): ReadonlyArray<PlanTask> =>
  view.plan.length > 0
    ? view.plan
    : view.steps.map((task) => (task.status === "error" ? { ...task, status: "complete" } : task))

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
        let { position, plan, steps } = view
        let changed = false
        for (;;) {
          const page = yield* runtime.steps({ worker: worker.runtimeRef!, afterPosition: position })
          const mine = page.steps.filter((step) => step.runId === runId)
          // A plan read again replaces the previous version: steps can be added, reworded or dropped.
          const planSteps = mine.filter((step) => step.kind === "plan_step")
          if (planSteps.length > 0) {
            plan = planSteps.map((step) => ({
              id: step.id,
              title: step.title,
              status: step.status,
              detail: null,
            }))
            changed = true
          }
          for (const step of mine) {
            if (!shownWithoutPlan.has(step.kind)) continue
            const task: PlanTask = { id: step.id, title: taskTitle(step), status: step.status, detail: null }
            const index = steps.findIndex((t) => t.id === step.id)
            steps = index === -1 ? [...steps, task] : steps.map((t, i) => (i === index ? task : t))
            changed = true
          }
          if (page.nextPosition === position) break
          position = page.nextPosition
        }
        return { view: { runId, position, plan, steps }, changed }
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
          known !== undefined && known.runId === runId ? known : { runId, position: 0, plan: [], steps: [] }
        const { view, changed } = yield* readSteps(worker, runId, base)
        yield* Ref.update(views, (all) => new Map(all).set(worker.id, view))
        if (changed) {
          yield* channel
            .upsertPlan({
              thread: parseThreadKey(worker.origin),
              plan: worker.planMessage,
              title: worker.brief.title,
              tasks: shownTasks(view),
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
          tasks: shownTasks(view),
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
        for (const worker of yield* store.workers.inStates(["launching"])) {
          yield* workers
            .resumeLaunch(worker.id)
            .pipe(Effect.catch((error) => Effect.logWarning(`launch of ${worker.id} still pending`, error)))
        }
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
