import { isAbsolute, relative } from "node:path"
import { Effect, Layer, Option } from "effect"
import { EventId, OperationId, type WorkerId } from "./domain/ids.ts"
import { parseThreadKey } from "./domain/thread.ts"
import { Channel } from "../ports/Channel.ts"
import { Memory } from "../ports/Memory.ts"
import { Store } from "../ports/Store.ts"
import { WorkerApi, WorkerApiError } from "../ports/WorkerApi.ts"
import { now } from "./clock.ts"
import { askOp, Dispatch } from "./Dispatch.ts"
import { Intake } from "./Intake.ts"
import { Outbox } from "./Outbox.ts"

/** The longest message a worker may post. Its full result goes through the router; `say` is for short answers. */
export const MAX_SAY = 3000
const MAX_IMAGES = 4
const IMAGE = /\.(png|jpe?g|gif)$/i

const fail = (message: string) => new WorkerApiError({ reason: "failed", message })

/** Why an image cannot go in a worker's request, or null when it can. */
const imageProblem = (path: string, worktree: string | null): string | null => {
  if (!IMAGE.test(path)) return `${path} is not a PNG, JPEG or GIF file`
  if (worktree === null) return "this worker has no worktree to attach images from"
  if (!isAbsolute(path)) return `${path} is not an absolute path`
  const inside = relative(worktree, path)
  // Images are the worker's own work, so they come from its worktree. The adapter checks again after symlinks.
  if (inside === "" || inside.startsWith("..") || isAbsolute(inside))
    return `${path} is outside your worktree`
  return null
}

/** What workers can ask of atta. Workers have no memory; this is where they reach atta's. */
export const WorkerApiLive = Layer.effect(
  WorkerApi,
  Effect.gen(function* () {
    const store = yield* Store
    const memory = yield* Memory
    const intake = yield* Intake
    const dispatch = yield* Dispatch
    const outbox = yield* Outbox
    const channel = yield* Channel

    const known = (workerId: WorkerId) =>
      store.workers.get(workerId).pipe(
        Effect.mapError((e) => new WorkerApiError({ reason: "failed", message: e.message })),
        Effect.flatMap((found) =>
          Option.match(found, {
            onNone: () =>
              Effect.fail(new WorkerApiError({ reason: "unknown_worker", message: `no worker ${workerId}` })),
            onSome: (worker) =>
              worker.state === "retired"
                ? Effect.fail(
                    new WorkerApiError({ reason: "worker_retired", message: `${workerId} is retired` }),
                  )
                : Effect.succeed(worker),
          }),
        ),
      )

    return WorkerApi.of({
      ask: ({ workerId, question }) =>
        Effect.gen(function* () {
          yield* known(workerId)
          const found = yield* memory
            .recall(question)
            .pipe(Effect.mapError((e) => new WorkerApiError({ reason: "failed", message: e.message })))
          return found === ""
            ? "atta has no more context than your brief. Make the most reasonable choice, and say which in your report."
            : found
        }),
      report: ({ workerId, text }) =>
        Effect.gen(function* () {
          yield* known(workerId)
          const at = yield* now
          const id = EventId.make(`report:${workerId}:${at}:${crypto.randomUUID()}`)
          const routed = yield* intake
            .accept({
              _tag: "WorkerReport",
              id,
              source: "worker",
              externalId: id,
              receivedAt: at,
              workerId,
              text,
            })
            .pipe(Effect.mapError((e) => new WorkerApiError({ reason: "failed", message: e.message })))
          if (routed !== null) yield* dispatch.deliver(id)
        }),
      say: ({ workerId, text }) =>
        Effect.gen(function* () {
          if (text.length > MAX_SAY) {
            return yield* new WorkerApiError({
              reason: "failed",
              message: `say is for short messages (up to ${MAX_SAY} characters); this one has ${text.length}`,
            })
          }
          const worker = yield* known(workerId)
          // Only a worker in the middle of a run talks; a call for an idle or stopping one is not from its run.
          if (worker.state !== "running") {
            return yield* new WorkerApiError({
              reason: "failed",
              message: `${workerId} is ${worker.state}, not running`,
            })
          }
          const operationId = OperationId.make(`say:${workerId}:${crypto.randomUUID()}`)
          const target = { thread: parseThreadKey(worker.origin) }
          yield* store.transaction(outbox.enqueue(operationId, "slack_reply", { target, text })).pipe(
            Effect.andThen(outbox.run(operationId)),
            Effect.mapError((e) => new WorkerApiError({ reason: "failed", message: e.message })),
          )
        }),
      askApproval: ({ workerId, text, images }) =>
        Effect.gen(function* () {
          if (text.trim() === "") return yield* fail("the request has no text")
          // Refused rather than cut, so the user never approves a request they could not read in full.
          if (!channel.askFits(text))
            return yield* fail("the request is too long for one Slack message; shorten it")
          if (images.length > MAX_IMAGES) return yield* fail(`a request has up to ${MAX_IMAGES} images`)
          const worker = yield* known(workerId)
          if (worker.state !== "running") return yield* fail(`${workerId} is ${worker.state}, not running`)
          for (const image of images) {
            const problem = imageProblem(image, worker.worktreePath)
            if (problem !== null) return yield* fail(problem)
          }
          const at = yield* now
          const askId = EventId.make(`ask:${workerId}:${at}:${crypto.randomUUID()}`)
          const operationId = askOp(askId)
          // The request and its post are written together, so a stored request is always posted, even after a crash.
          yield* store
            .transaction(
              Effect.gen(function* () {
                yield* store.events.insert({
                  _tag: "WorkerAsked",
                  id: askId,
                  source: "worker",
                  externalId: askId,
                  receivedAt: at,
                  workerId,
                  text,
                  images,
                })
                yield* store.events.markDispatched(askId, at)
                yield* outbox.enqueue(operationId, "ask_post", {
                  thread: parseThreadKey(worker.origin),
                  askId,
                  text,
                  images,
                  worktree: worker.worktreePath,
                })
              }),
            )
            .pipe(
              Effect.andThen(outbox.run(operationId)),
              Effect.mapError((e) => fail(e.message)),
            )
          return askId
        }),
    })
  }),
)
