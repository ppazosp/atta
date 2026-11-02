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
  readonly runs: number
  readonly steps: ReadonlyArray<Step>
  readonly status: RunStatus
}

const runId = (worker: RuntimeRef, n: number) => RunId.make(`${worker}:run:${n}`)

/**
 * Lets a test play the runtime: add steps, finish a run, fail the next launch, and read what atta asked for.
 * Launches are idempotent per operation ID, like T3's `clientRequestId`.
 */
export class FakeRuntimeControl extends Context.Service<
  FakeRuntimeControl,
  {
    addSteps(worker: RuntimeRef, steps: ReadonlyArray<Step>): Effect.Effect<void>
    setStatus(worker: RuntimeRef, status: Partial<RunStatus>): Effect.Effect<void>
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

    const runtime = Runtime.of({
      launch: ({ repo, brief, operationId }) =>
        Effect.gen(function* () {
          yield* record({ _tag: "launch", repo, briefText: brief.text, operationId })
          const failure = yield* Ref.getAndSet(launchFailure, null)
          if (failure !== null) return yield* new RuntimeError({ operation: "launch", message: failure })
          const existing = (yield* Ref.get(launches)).get(operationId)
          const worker = existing ?? RuntimeRef.make(`fake-thread-${(yield* Ref.get(launches)).size + 1}`)
          if (!existing) {
            yield* Ref.update(launches, (all) => new Map(all).set(operationId, worker))
            yield* put(worker, {
              runs: 1,
              steps: [],
              status: {
                runId: runId(worker, 1),
                state: "running",
                outcome: null,
                finalMessage: null,
                currentBranch: `atta/${worker}`,
                headCommit: null,
              },
            })
          }
          return {
            runtimeRef: worker,
            runId: runId(worker, 1),
            branch: `atta/${worker}`,
            worktreePath: `/tmp/${worker}`,
          }
        }),
      send: ({ worker, text, mode }) =>
        Effect.gen(function* () {
          yield* record({ _tag: "send", worker, text, mode })
          const script = yield* get(worker, "send")
          if (mode === "steer") return { runId: script.status.runId }
          const runs = script.runs + 1
          yield* put(worker, {
            ...script,
            runs,
            status: { ...script.status, runId: runId(worker, runs), state: "running", outcome: null },
          })
          return { runId: runId(worker, runs) }
        }),
      stop: ({ worker }) =>
        Effect.gen(function* () {
          yield* record({ _tag: "stop", worker })
          const script = yield* get(worker, "stop")
          yield* put(worker, {
            ...script,
            status: { ...script.status, state: "finished", outcome: "stopped" },
          })
        }),
      steps: ({ worker, afterPosition }) =>
        get(worker, "steps").pipe(
          Effect.map((script) => {
            const steps = script.steps.filter((s) => s.position > afterPosition)
            return { steps, nextPosition: steps.at(-1)?.position ?? afterPosition }
          }),
        ),
      status: ({ worker }) => get(worker, "status").pipe(Effect.map((script) => script.status)),
    })

    const control = FakeRuntimeControl.of({
      addSteps: (worker, steps) =>
        get(worker, "addSteps").pipe(
          Effect.flatMap((script) => put(worker, { ...script, steps: [...script.steps, ...steps] })),
          Effect.orDie,
        ),
      setStatus: (worker, status) =>
        get(worker, "setStatus").pipe(
          Effect.flatMap((script) => put(worker, { ...script, status: { ...script.status, ...status } })),
          Effect.orDie,
        ),
      failNextLaunch: (message) => Ref.set(launchFailure, message),
      calls: Ref.get(calls),
    })
    return Context.make(Runtime, runtime).pipe(Context.add(FakeRuntimeControl, control))
  }),
)
