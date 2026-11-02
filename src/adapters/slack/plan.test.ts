import { describe, expect, it } from "@effect/vitest"
import { Deferred, Effect, Fiber, Result, Schema } from "effect"
import { TestClock } from "effect/testing"
import { OperationId } from "../../core/domain/ids.ts"
import type { PlanTask } from "../../ports/Channel.ts"
import { Channel } from "../../ports/Channel.ts"
import { makeFakeSlack, plan, thread } from "./testing.ts"

const operationId = OperationId.make("plan-op")
const task = (id: number, status: PlanTask["status"] = "complete"): PlanTask => ({
  id: `task-${id}`,
  title: `Task ${id}`,
  status,
  detail: null,
})
const initial = { thread, plan: null, title: "Fix the issue", tasks: [task(1, "in_progress")], operationId }
const update = (title: string) => ({ ...initial, plan, title })
const finish = {
  plan,
  title: initial.title,
  tasks: [task(1)],
  outcome: "completed" as const,
  answer: "Fixed it.",
}
const PlanBlock = Schema.Struct({
  type: Schema.Literal("plan"),
  title: Schema.String,
  tasks: Schema.Array(
    Schema.Struct({
      type: Schema.Literal("task_card"),
      task_id: Schema.String,
      title: Schema.String,
      status: Schema.String,
      details: Schema.optionalKey(Schema.Unknown),
    }),
  ),
})
const blocksOf = (input: unknown) =>
  Schema.decodeUnknownSync(Schema.Struct({ blocks: Schema.Array(Schema.Unknown) }))(input).blocks
const planOf = (input: unknown) => Schema.decodeUnknownSync(PlanBlock)(blocksOf(input)[0])

describe("Slack plans", () => {
  it.effect("posts a tagged plan once with task cards and rich text details", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      expect(
        yield* channel.upsertPlan({
          ...initial,
          tasks: [
            task(1, "pending"),
            { ...task(2, "in_progress"), detail: "Running pnpm check" },
            task(3),
            task(4, "error"),
          ],
        }),
      ).toEqual(plan)
      expect(fake.calls.post).toHaveLength(1)
      expect(fake.calls.post[0]).toMatchObject({
        channel: thread.conversation,
        thread_ts: thread.rootTs,
        text: initial.title,
        metadata: { event_type: "atta_operation", event_payload: { operation_id: "plan-op" } },
      })
      expect(planOf(fake.calls.post[0])).toEqual({
        type: "plan",
        title: initial.title,
        tasks: [
          { type: "task_card", task_id: "task-1", title: "Task 1", status: "pending" },
          {
            type: "task_card",
            task_id: "task-2",
            title: "Task 2",
            status: "in_progress",
            details: {
              type: "rich_text",
              elements: [
                { type: "rich_text_section", elements: [{ type: "text", text: "Running pnpm check" }] },
              ],
            },
          },
          { type: "task_card", task_id: "task-3", title: "Task 3", status: "complete" },
          { type: "task_card", task_id: "task-4", title: "Task 4", status: "error" },
        ],
      })
      expect(blocksOf(fake.calls.post[0])).toHaveLength(1)
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("coalesces edits to the latest content and spaces writes at least one second apart", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* channel.upsertPlan(initial)
      const first = yield* channel.upsertPlan(update("First edit")).pipe(Effect.forkChild)
      const latest = yield* channel.upsertPlan(update("Latest edit")).pipe(Effect.forkChild)
      yield* TestClock.adjust(999)
      expect(fake.calls.update).toHaveLength(0)
      yield* TestClock.adjust(1)
      expect(yield* Fiber.join(first)).toEqual(plan)
      expect(yield* Fiber.join(latest)).toEqual(plan)
      expect(fake.calls.post).toHaveLength(1)
      expect(fake.calls.update).toHaveLength(1)
      expect(fake.calls.update[0]).toMatchObject({
        channel: plan.conversation,
        ts: plan.ts,
        text: "Latest edit",
      })
      const next = yield* channel.upsertPlan(update("Next edit")).pipe(Effect.forkChild)
      yield* TestClock.adjust(999)
      expect(fake.calls.update).toHaveLength(1)
      yield* TestClock.adjust(1)
      yield* Fiber.join(next)
      expect(fake.calls.update).toHaveLength(2)
      expect(planOf(fake.calls.update[1]).title).toBe("Next edit")
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("edits a recovered plan immediately without reposting", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      expect(yield* channel.upsertPlan(update("Recovered"))).toEqual(plan)
      expect(fake.calls.post).toHaveLength(0)
      expect(fake.calls.update).toHaveLength(1)
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("keeps the writer alive when a caller is interrupted", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* channel.upsertPlan(initial)
      const cancelled = yield* channel.upsertPlan(update("Cancelled caller")).pipe(Effect.forkChild)
      yield* TestClock.adjust(0)
      yield* Fiber.interrupt(cancelled)
      const latest = yield* channel.upsertPlan(update("Latest content")).pipe(Effect.forkChild)
      yield* TestClock.adjust(1_000)
      yield* Fiber.join(latest)
      expect(fake.calls.update).toHaveLength(1)
      expect(planOf(fake.calls.update[0]).title).toBe("Latest content")
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("cancels pending writes when the layer closes", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      yield* Effect.gen(function* () {
        const channel = yield* Channel
        yield* channel.upsertPlan(initial)
        yield* channel.upsertPlan(update("Pending")).pipe(Effect.forkChild)
        yield* TestClock.adjust(0)
      }).pipe(Effect.provide(fake.layer))
      yield* TestClock.adjust(1_000)
      expect(fake.lifecycle.stopped).toBe(1)
      expect(fake.calls.update).toHaveLength(0)
    })
  })

  it.effect("throttles each message independently", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* channel.upsertPlan(update("Message one"))
      yield* channel.upsertPlan({ ...update("Message two"), plan: { ...plan, ts: "different" } })
      expect(fake.calls.update).toHaveLength(2)
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("serializes slow API calls and applies updates received during an in-flight write", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      const started = yield* Deferred.make<void>()
      let release: ((value: unknown) => void) | undefined
      fake.responses.update = () => {
        Deferred.doneUnsafe(started, Effect.void)
        return new Promise((resolve) => {
          release = resolve
        })
      }
      const first = yield* channel.upsertPlan(update("In flight")).pipe(Effect.forkChild)
      yield* Deferred.await(started)
      const second = yield* channel.upsertPlan(update("Superseded")).pipe(Effect.forkChild)
      const third = yield* channel.upsertPlan(update("Newest")).pipe(Effect.forkChild)
      yield* TestClock.adjust(1_500)
      expect(fake.calls.update).toHaveLength(1)
      fake.responses.update = async () => ({ ok: true })
      release!({ ok: true })
      yield* Fiber.join(first)
      yield* Fiber.join(second)
      yield* Fiber.join(third)
      expect(fake.calls.update).toHaveLength(2)
      expect(planOf(fake.calls.update[1]).title).toBe("Newest")
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("preserves exactly 50 tasks without grouping", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      const tasks = Array.from({ length: 50 }, (_, i) => task(i))
      yield* channel.upsertPlan({ ...initial, tasks })
      expect(planOf(fake.calls.post[0]).tasks.map((t) => t.task_id)).toEqual(tasks.map((t) => t.id))
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("groups the oldest finished tasks, keeps unfinished tasks and preserves failures", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      const tasks = [
        task(0, "in_progress"),
        task(1, "error"),
        task(2),
        ...Array.from({ length: 49 }, (_, i) => task(i + 3)),
      ]
      yield* channel.upsertPlan({ ...initial, tasks })
      const actual = planOf(fake.calls.post[0]).tasks
      expect(actual).toHaveLength(50)
      expect(actual[0]).toMatchObject({ title: "3 earlier tasks finished (1 failed)", status: "error" })
      expect(actual.slice(1).map((t) => t.task_id)).toEqual(["task-0", ...tasks.slice(4).map((t) => t.id)])
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("marks a summary complete when all grouped tasks succeeded", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* channel.upsertPlan({ ...initial, tasks: Array.from({ length: 51 }, (_, i) => task(i)) })
      expect(planOf(fake.calls.post[0]).tasks[0]).toMatchObject({
        title: "2 earlier tasks finished",
        status: "complete",
      })
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("uses a summary ID that cannot collide with a worker task", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      const tasks = [
        { ...task(0, "in_progress"), id: "atta_finished_summary" },
        ...Array.from({ length: 50 }, (_, i) => task(i + 1)),
      ]
      yield* channel.upsertPlan({ ...initial, tasks })
      const ids = planOf(fake.calls.post[0]).tasks.map((t) => t.task_id)
      expect(ids).toHaveLength(50)
      expect(new Set(ids).size).toBe(50)
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("fails explicitly when active tasks cannot fit under the task limit", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      const result = yield* channel
        .upsertPlan({ ...initial, tasks: Array.from({ length: 51 }, (_, i) => task(i, "in_progress")) })
        .pipe(Effect.result)
      expect(Result.isFailure(result) && result.failure).toMatchObject({
        _tag: "ChannelError",
        operation: "plan",
      })
      expect(fake.calls.post).toHaveLength(0)
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect.each(["completed", "failed", "interrupted", "stopped"] as const)(
    "finishes with outcome %s and the answer below the plan",
    (outcome) => {
      const fake = makeFakeSlack()
      return Effect.gen(function* () {
        const channel = yield* Channel
        yield* channel.finishPlan({ ...finish, outcome })
        expect(fake.calls.post).toHaveLength(0)
        expect(fake.calls.update).toHaveLength(1)
        expect(planOf(fake.calls.update[0])).toMatchObject({
          title: `${initial.title} — ${outcome}`,
          tasks: [{ status: "complete" }],
        })
        expect(blocksOf(fake.calls.update[0])[1]).toEqual({
          type: "rich_text",
          elements: [{ type: "rich_text_section", elements: [{ type: "text", text: finish.answer }] }],
        })
        expect(fake.calls.update[0]).toMatchObject({
          text: `${initial.title} — ${outcome}\n\n${finish.answer}`,
        })
      }).pipe(Effect.provide(fake.layer))
    },
  )

  it.effect("finishes without an answer block when the worker has no answer", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* channel.finishPlan({ ...finish, answer: null })
      expect(blocksOf(fake.calls.update[0])).toHaveLength(1)
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect(
    "coalesces a pending progress edit into the final state and prevents late progress overwrites",
    () => {
      const fake = makeFakeSlack()
      return Effect.gen(function* () {
        const channel = yield* Channel
        yield* channel.upsertPlan(initial)
        const progress = yield* channel.upsertPlan(update("Still running")).pipe(Effect.forkChild)
        const done = yield* channel.finishPlan(finish).pipe(Effect.forkChild)
        yield* TestClock.adjust(999)
        expect(fake.calls.update).toHaveLength(0)
        yield* TestClock.adjust(1)
        yield* Fiber.join(progress)
        yield* Fiber.join(done)
        yield* channel.upsertPlan(update("Stale progress"))
        expect(fake.calls.update).toHaveLength(1)
        expect(planOf(fake.calls.update[0]).title).toContain("completed")
      }).pipe(Effect.provide(fake.layer))
    },
  )

  it.effect("delivers write failures to every coalesced caller, then accepts a retry", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      yield* channel.upsertPlan(initial)
      fake.responses.update = () => Promise.reject({ data: { error: "ratelimited" } })
      const first = yield* channel.upsertPlan(update("First")).pipe(Effect.result, Effect.forkChild)
      const second = yield* channel.upsertPlan(update("Second")).pipe(Effect.result, Effect.forkChild)
      yield* TestClock.adjust(1_000)
      for (const result of [yield* Fiber.join(first), yield* Fiber.join(second)]) {
        expect(Result.isFailure(result) && result.failure).toMatchObject({
          _tag: "ChannelError",
          operation: "chat.update",
          message: "ratelimited",
        })
      }
      fake.responses.update = async () => ({ ok: true })
      const retry = yield* channel.upsertPlan(update("Retry")).pipe(Effect.forkChild)
      yield* TestClock.adjust(1_000)
      yield* Fiber.join(retry)
      expect(planOf(fake.calls.update[1]).title).toBe("Retry")
    }).pipe(Effect.provide(fake.layer))
  })

  it.effect("allows retrying a failed final write", () => {
    const fake = makeFakeSlack()
    return Effect.gen(function* () {
      const channel = yield* Channel
      fake.responses.update = async () => ({ ok: false, error: "internal_error" })
      const result = yield* channel.finishPlan(finish).pipe(Effect.result)
      expect(Result.isFailure(result) && result.failure).toMatchObject({ message: "internal_error" })
      yield* channel.upsertPlan(update("Late progress"))
      expect(fake.calls.update).toHaveLength(1)
      fake.responses.update = async () => ({ ok: true })
      const retry = yield* channel.finishPlan(finish).pipe(Effect.forkChild)
      yield* TestClock.adjust(1_000)
      yield* Fiber.join(retry)
      expect(fake.calls.update).toHaveLength(2)
    }).pipe(Effect.provide(fake.layer))
  })
})
