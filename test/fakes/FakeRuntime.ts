import { Context, Effect, Layer, Ref } from "effect"
import { RunId, RuntimeRef } from "../../src/core/domain/ids.ts"
import type { Step } from "../../src/core/domain/worker.ts"
import type { RunStatus } from "../../src/ports/Runtime.ts"
import { Runtime, RuntimeError } from "../../src/ports/Runtime.ts"

export type RuntimeCall =
  | {
      readonly _tag: "launch"
      readonly repo: string
      readonly briefText: string
      readonly operationId: string
    }
  | {
      readonly _tag: "send"
      readonly worker: string
      readonly text: string
      readonly mode: "steer" | "queue"
    }
  | { readonly _tag: "stop"; readonly worker: string }

interface WorkerScript {
  readonly runs: ReadonlyArray<RunStatus>
  readonly steps: ReadonlyArray<Step>
}

const runIdOf = (worker: RuntimeRef, n: number) => RunId.make(`${worker}:run:${n}`)
const branchOf = (worker: RuntimeRef) => `atta/${worker}`

const newRun = (worker: RuntimeRef, n: number): RunStatus => ({
  runId: runIdOf(worker, n),
  state: "running",
  outcome: null,
  finalMessage: null,
  assignedBranch: branchOf(worker),
  currentBranch: branchOf(worker),
  headCommit: null,
})

/**
 * Lets a test play the runtime. It behaves like T3 where core depends on it:
 * - a repeated launch or send with the same operation ID returns the first result without acting again
 * - launch does not report the branch; `status` does
 * - `steps` stops before the first unfinished step, so it is read again until it finishes
 * - `status` answers for the run asked about; `startContinuation` adds a run of the runtime's own, as T3 does
 *   after a restart, which must not change the status of the run atta started
 */
export class FakeRuntimeControl extends Context.Service<
  FakeRuntimeControl,
  {
    /** Adds steps, or replaces steps with the same ID (a running step that finished). */
    putSteps(worker: RuntimeRef, steps: ReadonlyArray<Step>): Effect.Effect<void>
    finishRun(worker: RuntimeRef, runId: RunId, status: Partial<RunStatus>): Effect.Effect<void>
    startContinuation(worker: RuntimeRef): Effect.Effect<RunId>
    failNextLaunch(message: string): Effect.Effect<void>
    readonly calls: Effect.Effect<ReadonlyArray<RuntimeCall>>
  }
>()("atta/test/FakeRuntimeControl") {}

export const FakeRuntimeLayer = Layer.effectContext(
  Effect.gen(function* () {
    const workers = yield* Ref.make<ReadonlyMap<string, WorkerScript>>(new Map())
    const launches = yield* Ref.make<ReadonlyMap<string, RuntimeRef>>(new Map())
    const calls = yield* Ref.make<ReadonlyArray<RuntimeCall>>([])
    const launchFailure = yield* Ref.make<string | null>(null)
    const sends = yield* Ref.make<ReadonlyMap<string, RunId>>(new Map())
    const record = (call: RuntimeCall) => Ref.update(calls, (all) => [...all, call])
    const get = (worker: RuntimeRef, operation: string) =>
      Ref.get(workers).pipe(
        Effect.flatMap((all) => {
          const script = all.get(worker)
          return script
            ? Effect.succeed(script)
            : Effect.fail(new RuntimeError({ operation, message: `unknown worker ${worker}` }))
        }),
      )
    const put = (worker: RuntimeRef, script: WorkerScript) =>
      Ref.update(workers, (all) => new Map(all).set(worker, script))
    const addRun = (worker: RuntimeRef, script: WorkerScript) => {
      const run = newRun(worker, script.runs.length + 1)
      return put(worker, { ...script, runs: [...script.runs, run] }).pipe(Effect.as(run.runId))
    }

    const runtime = Runtime.of({
      launch: ({ repo, brief, operationId }) =>
        Effect.gen(function* () {
          yield* record({ _tag: "launch", repo, briefText: brief.text, operationId })
          const failure = yield* Ref.getAndSet(launchFailure, null)
          if (failure !== null) return yield* new RuntimeError({ operation: "launch", message: failure })
          const known = yield* Ref.get(launches)
          const existing = known.get(operationId)
          if (existing)
            return { runtimeRef: existing, runId: runIdOf(existing, 1), branch: null, worktreePath: null }
          const worker = RuntimeRef.make(`fake-thread-${known.size + 1}`)
          yield* Ref.update(launches, (all) => new Map(all).set(operationId, worker))
          yield* put(worker, { runs: [newRun(worker, 1)], steps: [] })
          return { runtimeRef: worker, runId: runIdOf(worker, 1), branch: null, worktreePath: null }
        }),
      send: ({ worker, text, mode, operationId }) =>
        Effect.gen(function* () {
          const repeated = (yield* Ref.get(sends)).get(operationId)
          if (repeated !== undefined) return { runId: repeated }
          yield* record({ _tag: "send", worker, text, mode })
          const script = yield* get(worker, "send")
          const current = script.runs.at(-1)
          const runId =
            mode === "steer" && current?.state === "running" ? current.runId : yield* addRun(worker, script)
          yield* Ref.update(sends, (all) => new Map(all).set(operationId, runId))
          return { runId }
        }),
      stop: ({ worker }) =>
        Effect.gen(function* () {
          yield* record({ _tag: "stop", worker })
          const script = yield* get(worker, "stop")
          yield* put(worker, {
            ...script,
            runs: script.runs.map((r) =>
              r.state === "running" ? { ...r, state: "finished" as const, outcome: "stopped" as const } : r,
            ),
          })
        }),
      steps: ({ worker, afterPosition }) =>
        get(worker, "steps").pipe(
          Effect.map((script) => {
            const steps = script.steps.filter((s) => s.position > afterPosition)
            const unfinished = steps.find((s) => s.status === "pending" || s.status === "in_progress")
            const nextPosition =
              unfinished !== undefined ? unfinished.position - 1 : (steps.at(-1)?.position ?? afterPosition)
            return { steps, nextPosition: Math.max(nextPosition, afterPosition) }
          }),
        ),
      status: ({ worker, runId }) =>
        get(worker, "status").pipe(
          Effect.flatMap((script) => {
            const run = script.runs.find((r) => r.runId === runId)
            return run
              ? Effect.succeed(run)
              : Effect.fail(new RuntimeError({ operation: "status", message: `unknown run ${runId}` }))
          }),
        ),
    })

    const control = FakeRuntimeControl.of({
      putSteps: (worker, steps) =>
        get(worker, "putSteps").pipe(
          Effect.flatMap((script) => {
            const ids = new Set(steps.map((s) => s.id))
            const merged = [...script.steps.filter((s) => !ids.has(s.id)), ...steps].toSorted(
              (a, b) => a.position - b.position,
            )
            return put(worker, { ...script, steps: merged })
          }),
          Effect.orDie,
        ),
      finishRun: (worker, runId, status) =>
        get(worker, "finishRun").pipe(
          Effect.flatMap((script) =>
            put(worker, {
              ...script,
              runs: script.runs.map((r) =>
                r.runId === runId
                  ? { ...r, state: "finished" as const, outcome: "completed" as const, ...status }
                  : r,
              ),
            }),
          ),
          Effect.orDie,
        ),
      startContinuation: (worker) =>
        get(worker, "startContinuation").pipe(
          Effect.flatMap((script) => addRun(worker, script)),
          Effect.orDie,
        ),
      failNextLaunch: (message) => Ref.set(launchFailure, message),
      calls: Ref.get(calls),
    })
    return Context.make(Runtime, runtime).pipe(Context.add(FakeRuntimeControl, control))
  }),
)
