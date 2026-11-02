import { Context, Effect, Layer, Ref } from "effect"
import { WorkerApi, WorkerApiError } from "../../src/ports/WorkerApi.ts"

export type WorkerApiCall =
  | { readonly _tag: "ask"; readonly workerId: string; readonly question: string }
  | { readonly _tag: "report"; readonly workerId: string; readonly text: string }

/** Answers every question with `answer`, and records calls. Workers whose ID starts with `unknown` are rejected. */
export class FakeWorkerApiControl extends Context.Service<
  FakeWorkerApiControl,
  { readonly calls: Effect.Effect<ReadonlyArray<WorkerApiCall>> }
>()("atta/test/FakeWorkerApiControl") {}

const check = (workerId: string) =>
  workerId.startsWith("unknown")
    ? Effect.fail(new WorkerApiError({ reason: "unknown_worker", message: `no worker ${workerId}` }))
    : Effect.void

export const makeFakeWorkerApiLayer = (answer: string) =>
  Layer.effectContext(
    Effect.gen(function* () {
      const calls = yield* Ref.make<ReadonlyArray<WorkerApiCall>>([])
      const api = WorkerApi.of({
        ask: ({ workerId, question }) =>
          check(workerId).pipe(
            Effect.andThen(
              Ref.update(calls, (all) => [...all, { _tag: "ask" as const, workerId, question }]),
            ),
            Effect.as(answer),
          ),
        report: ({ workerId, text }) =>
          check(workerId).pipe(
            Effect.andThen(Ref.update(calls, (all) => [...all, { _tag: "report" as const, workerId, text }])),
          ),
      })
      return Context.make(WorkerApi, api).pipe(
        Context.add(FakeWorkerApiControl, FakeWorkerApiControl.of({ calls: Ref.get(calls) })),
      )
    }),
  )
