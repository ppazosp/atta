import { execFile } from "node:child_process"
import { promisify } from "node:util"
import { describe, expect, it } from "@effect/vitest"
import { Effect, Schema } from "effect"
import { OperationId, RunId, RuntimeRef } from "../../core/domain/ids.ts"
import { Runtime, RuntimeError } from "../../ports/Runtime.ts"
import { T3RuntimeLayer } from "./index.ts"
import { LaunchResult, StatusPage } from "./schemas.ts"
import { config, fixture, replay, temporaryDirectory } from "./test-support.ts"

const launchInput = (args: Record<string, unknown>) =>
  Schema.decodeUnknownSync(
    Schema.Struct({
      title: Schema.String,
      message: Schema.String,
      clientRequestId: OperationId,
    }),
  )(args)

describe("T3 runtime", () => {
  it.effect("launches in a worktree with the model, full access and a repeatable request ID", () =>
    Effect.gen(function* () {
      const first = yield* fixture("run/02-launch")
      const repeated = yield* fixture("run/03-launch-repeated")
      const { layer, calls } = yield* replay([first, repeated])
      yield* Effect.gen(function* () {
        const runtime = yield* Runtime
        const input = launchInput(first.args)
        const request = {
          repo: "sandbox-repo",
          brief: { repo: "sandbox-repo", title: input.title, text: input.message },
          operationId: input.clientRequestId,
        }
        const launched = yield* runtime.launch(request)
        expect(yield* runtime.launch(request)).toEqual(launched)
        const recorded = yield* Schema.decodeUnknownEffect(LaunchResult)(first.data)
        expect(launched).toEqual({
          runtimeRef: recorded.threadId,
          runId: recorded.runId,
          branch: null,
          worktreePath: null,
        })
        expect(calls).toEqual([0, 1].map(() => ({ name: first.tool, arguments: first.args })))
      }).pipe(Effect.provide(layer))
    }).pipe(Effect.scoped),
  )

  it.effect.each(["07-steer", "13-follow-up-queue"])(
    "sends %s with its operation ID and returns T3's run",
    (name) =>
      Effect.gen(function* () {
        const recorded = yield* fixture(`run/${name}`)
        const { layer, calls } = yield* replay([recorded], { textOnly: true })
        const args = yield* Schema.decodeUnknownEffect(
          Schema.Struct({
            threadId: RuntimeRef,
            mode: Schema.Literals(["steer", "queue"]),
            clientRequestId: OperationId,
            message: Schema.String,
          }),
        )(recorded.args)
        const result = yield* Effect.gen(function* () {
          return yield* (yield* Runtime).send({
            worker: args.threadId,
            text: args.message,
            mode: args.mode,
            operationId: args.clientRequestId,
          })
        }).pipe(Effect.provide(layer))
        expect(result).toEqual({ runId: recorded.data.runId })
        expect(calls).toEqual([{ name: recorded.tool, arguments: recorded.args }])
      }).pipe(Effect.scoped),
  )

  it.effect("stops with stop true and the operation ID", () =>
    Effect.gen(function* () {
      const recorded = yield* fixture("run/17-stop")
      const { layer, calls } = yield* replay([recorded])
      yield* Effect.gen(function* () {
        yield* (yield* Runtime).stop({
          worker: RuntimeRef.make(String(recorded.args.threadId)),
          operationId: OperationId.make("fixture-stop-int-1"),
        })
      }).pipe(Effect.provide(layer))
      expect(calls).toEqual([
        {
          name: recorded.tool,
          arguments: { threadId: recorded.args.threadId, stop: true, clientRequestId: "fixture-stop-int-1" },
        },
      ])
    }).pipe(Effect.scoped),
  )

  it.effect("can repeat a stop when T3 has no active run", () =>
    Effect.gen(function* () {
      const recorded = yield* fixture("run/17-stop")
      const stopped = { ...recorded, data: { ...recorded.data, runId: null, status: "no_active_run" } }
      const { layer, calls } = yield* replay([recorded, stopped])
      yield* Effect.gen(function* () {
        const runtime = yield* Runtime
        const request = {
          worker: RuntimeRef.make(String(recorded.args.threadId)),
          operationId: OperationId.make("fixture-stop-int-1"),
        }
        yield* runtime.stop(request)
        yield* runtime.stop(request)
      }).pipe(Effect.provide(layer))
      expect(calls[0]).toEqual(calls[1])
    }).pipe(Effect.scoped),
  )

  it.effect("uses each repository's project and base branch and the configured worker model", () =>
    Effect.gen(function* () {
      const recorded = yield* fixture("run/02-launch")
      const { layer, calls } = yield* replay([recorded], {
        runtimeConfig: {
          ...config,
          instanceId: "fixture-provider",
          model: "fixture-model",
          effort: "xhigh",
          repos: [
            ...config.repos,
            { name: "another-repo", t3ProjectId: "fixture-project", baseBranch: "develop" },
          ],
        },
      })
      yield* Effect.gen(function* () {
        yield* (yield* Runtime).launch({
          repo: "another-repo",
          brief: { repo: "another-repo", title: "another task", text: "fixture" },
          operationId: OperationId.make("another-operation"),
        })
      }).pipe(Effect.provide(layer))
      expect(calls[0]?.arguments).toMatchObject({
        projectId: "fixture-project",
        workspaceStrategy: { type: "worktree", baseRef: "develop" },
        modelSelection: {
          instanceId: "fixture-provider",
          model: "fixture-model",
          options: [{ id: "reasoningEffort", value: "xhigh" }],
        },
      })
    }).pipe(Effect.scoped),
  )

  it.effect("pages activity, rereads unfinished items and preserves the position on null last pages", () =>
    Effect.gen(function* () {
      const first = yield* fixture("run/04-activity-page-0")
      const running = yield* fixture("run/05-activity-page-1")
      const empty = yield* fixture("run/06-activity-page-2")
      const finished = yield* fixture("run/09-activity-after-finish")
      const last = yield* fixture("run/10-activity-last-page")
      const completedRunning = {
        ...running,
        data: {
          ...running.data,
          items: Schema.decodeUnknownSync(StatusPage)(running.data).items.map((item) => ({
            ...item,
            status: "completed",
          })),
        },
      }
      const { layer, calls } = yield* replay([
        first,
        running,
        running,
        empty,
        completedRunning,
        finished,
        last,
      ])
      yield* Effect.gen(function* () {
        const runtime = yield* Runtime
        const worker = RuntimeRef.make(String(first.args.threadId))
        const page1 = yield* runtime.steps({ worker, afterPosition: 0 })
        expect(page1.nextPosition).toBe(1)
        expect(page1.steps[0]).toMatchObject({
          kind: "command",
          title: "Workspace ready",
          status: "complete",
          position: 1,
        })
        const page2 = yield* runtime.steps({ worker, afterPosition: page1.nextPosition })
        expect(page2.nextPosition).toBe(2)
        expect(page2.steps.map((step) => [step.position, step.kind, step.status])).toEqual([
          [2, "message", "complete"],
          [3, "command", "in_progress"],
        ])
        const again = yield* runtime.steps({ worker, afterPosition: page2.nextPosition })
        expect(again.nextPosition).toBe(2)
        expect(again.steps.at(-1)?.id).toBe(page2.steps.at(-1)?.id)
        expect(yield* runtime.steps({ worker, afterPosition: 3 })).toEqual({ steps: [], nextPosition: 3 })
        const complete = yield* runtime.steps({ worker, afterPosition: 2 })
        expect(complete.nextPosition).toBe(3)
        expect(complete.steps.at(-1)).toMatchObject({ id: page2.steps.at(-1)?.id, status: "complete" })
        const page3 = yield* runtime.steps({ worker, afterPosition: 3 })
        expect(page3.nextPosition).toBe(8)
        expect(page3.steps.at(-1)?.kind).toBe("checkpoint")
        expect(yield* runtime.steps({ worker, afterPosition: 100000 })).toEqual({
          steps: [],
          nextPosition: 100000,
        })
        expect(calls.map((call) => call.arguments?.afterPosition)).toEqual([0, 1, 2, 3, 2, 3, 100000])
        expect(calls.every((call) => call.arguments?.view === "activity")).toBe(true)
      }).pipe(Effect.provide(layer))
    }).pipe(Effect.scoped),
  )

  it.effect.each([
    ["run/11-messages", "completed"],
    ["run/19-stop-activity", "stopped"],
    ["restart/b-03-interrupted-messages", "interrupted"],
  ] as const)("reports the requested run from %s as %s", ([name, outcome]) =>
    Effect.gen(function* () {
      const recorded = yield* fixture(name)
      const page = yield* Schema.decodeUnknownEffect(StatusPage)(recorded.data)
      const run = page.recentRuns.at(-1)!
      const { layer } = yield* replay([recorded])
      const status = yield* Effect.gen(function* () {
        return yield* (yield* Runtime).status({ worker: page.thread.threadId, runId: run.runId })
      }).pipe(Effect.provide(layer))
      expect(status).toEqual({
        runId: run.runId,
        state: "finished",
        outcome,
        finalMessage:
          page.items.filter((item) => item.runId === run.runId && item.type === "assistant_message").at(-1)
            ?.text ?? null,
        assignedBranch: page.thread.branch,
        currentBranch: null,
        headCommit: null,
      })
      if (outcome === "interrupted")
        expect(status.finalMessage).not.toContain("Interrupted before I could confirm completion.")
    }).pipe(Effect.scoped),
  )

  it.effect("turns Codex's plan into plan steps, and rereads the plan while steps remain", () =>
    Effect.gen(function* () {
      const recorded = yield* fixture("run/04-activity-page-0")
      const page = yield* Schema.decodeUnknownEffect(StatusPage)(recorded.data)
      const source = page.items[0]!
      const items = [
        {
          ...source,
          itemId: "turn-plan",
          type: "todo_list",
          status: "running",
          title: null,
          text: "Add div with a zero check\n[completed] Read calc.py\n[running] Add div()\n[pending] Add tests",
          position: 1,
        },
        { ...source, itemId: "cmd", type: "command_execution", status: "completed", position: 2 },
      ]
      const { layer } = yield* replay([{ ...recorded, data: { ...recorded.data, items, nextPosition: 2 } }])
      const result = yield* Effect.gen(function* () {
        return yield* (yield* Runtime).steps({ worker: page.thread.threadId, afterPosition: 0 })
      }).pipe(Effect.provide(layer))
      expect(result.steps.filter((s) => s.kind === "plan_step").map((s) => [s.title, s.status])).toEqual([
        ["Read calc.py", "complete"],
        ["Add div()", "in_progress"],
        ["Add tests", "pending"],
      ])
      // The plan still has open steps, so the next read starts before it again.
      expect(result.nextPosition).toBe(0)
    }).pipe(Effect.scoped),
  )

  it.effect("maps waiting, empty-text and unknown activity items without skipping unfinished work", () =>
    Effect.gen(function* () {
      const recorded = yield* fixture("run/04-activity-page-0")
      const page = yield* Schema.decodeUnknownEffect(StatusPage)(recorded.data)
      const source = page.items[0]!
      const items = [
        { ...source, itemId: "unassigned", runId: null, position: 1 },
        {
          ...source,
          itemId: "waiting",
          type: "reasoning",
          status: "waiting",
          title: null,
          text: null,
          position: 2,
        },
        { ...source, itemId: "file", type: "file_change", status: "pending", position: 3 },
        {
          ...source,
          itemId: "other",
          type: "new_t3_type",
          status: "failed",
          title: "a".repeat(200),
          position: 4,
        },
      ]
      const { layer } = yield* replay([{ ...recorded, data: { ...recorded.data, items, nextPosition: 4 } }])
      const result = yield* Effect.gen(function* () {
        return yield* (yield* Runtime).steps({ worker: page.thread.threadId, afterPosition: 0 })
      }).pipe(Effect.provide(layer))
      expect(result.nextPosition).toBe(1)
      expect(result.steps.map((step) => [step.id, step.kind, step.status])).toEqual([
        ["waiting", "reasoning", "in_progress"],
        ["file", "file_change", "pending"],
        ["other", "other", "error"],
      ])
      expect(result.steps[0]).toMatchObject({ title: "reasoning", detail: null })
      expect(result.steps[2]?.title.length).toBeLessThanOrEqual(120)
    }).pipe(Effect.scoped),
  )

  it.effect("pages messages to find the requested run's last assistant message", () =>
    Effect.gen(function* () {
      const recorded = yield* fixture("run/15-follow-up-messages")
      const page = yield* Schema.decodeUnknownEffect(StatusPage)(recorded.data)
      const next = {
        ...recorded,
        data: {
          ...recorded.data,
          items: [
            { ...page.items.at(-1)!, position: 10, runId: page.recentRuns[0]!.runId, text: "follow-up ok" },
          ],
          hasMore: false,
          nextPosition: null,
        },
      }
      const { layer, calls } = yield* replay([recorded, next])
      const status = yield* Effect.gen(function* () {
        return yield* (yield* Runtime).status({
          worker: page.thread.threadId,
          runId: page.recentRuns[0]!.runId,
        })
      }).pipe(Effect.provide(layer))
      expect(status.finalMessage).toBe("follow-up ok")
      expect(calls[1]?.arguments?.afterPosition).toBe(7)
    }).pipe(Effect.scoped),
  )

  it.effect("reads every text chunk of the last assistant message of the requested run", () =>
    Effect.gen(function* () {
      const recorded = yield* fixture("run/11-messages")
      const page = yield* Schema.decodeUnknownEffect(StatusPage)(recorded.data)
      const lastMessage = page.items.at(-1)!
      const first = {
        ...recorded,
        data: {
          ...recorded.data,
          items: [{ ...lastMessage, text: "first ", textTruncated: true, nextTextOffset: 6 }],
        },
      }
      const second = {
        ...recorded,
        data: {
          ...recorded.data,
          items: [{ ...lastMessage, text: "second ", textTruncated: true, nextTextOffset: 13 }],
        },
      }
      const third = {
        ...recorded,
        data: {
          ...recorded.data,
          items: [{ ...lastMessage, text: "third", textTruncated: false, nextTextOffset: null }],
        },
      }
      const { layer, calls } = yield* replay([first, second, third])
      const result = yield* Effect.gen(function* () {
        return yield* (yield* Runtime).status({
          worker: page.thread.threadId,
          runId: page.recentRuns[0]!.runId,
        })
      }).pipe(Effect.provide(layer))
      expect(result.finalMessage).toBe("first second third")
      expect(calls.slice(1).map((call) => [call.arguments?.itemId, call.arguments?.textOffset])).toEqual([
        [lastMessage.itemId, 6],
        [lastMessage.itemId, 13],
      ])
    }).pipe(Effect.scoped),
  )

  it.effect.each(["failed", "error", "preparing", "starting", "running"] as const)(
    "maps run status %s",
    (status) =>
      Effect.gen(function* () {
        const recorded = yield* fixture("run/11-messages")
        const page = yield* Schema.decodeUnknownEffect(StatusPage)(recorded.data)
        const { layer } = yield* replay([
          { ...recorded, data: { ...recorded.data, recentRuns: [{ ...page.recentRuns[0]!, status }] } },
        ])
        const result = yield* Effect.gen(function* () {
          return yield* (yield* Runtime).status({
            worker: page.thread.threadId,
            runId: page.recentRuns[0]!.runId,
          })
        }).pipe(Effect.provide(layer))
        const failed = status === "failed" || status === "error"
        expect(result.state).toBe(failed ? "finished" : "running")
        expect(result.outcome).toBe(failed ? "failed" : null)
      }).pipe(Effect.scoped),
  )

  it.effect("reads the actual worktree branch and commit after a worker changes branches", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const execute = promisify(execFile)
      const git = (...args: Array<string>) =>
        Effect.tryPromise(() => execute("git", ["-C", directory, ...args]))
      yield* git("init", "-b", "assigned")
      yield* git(
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.test",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "--allow-empty",
        "-m",
        "fixture",
      )
      yield* git("switch", "-c", "changed")
      const head = (yield* git("rev-parse", "HEAD")).stdout.trim()
      const recorded = yield* fixture("run/11-messages")
      const page = yield* Schema.decodeUnknownEffect(StatusPage)(recorded.data)
      const response = {
        ...recorded,
        data: { ...recorded.data, thread: { ...page.thread, branch: "assigned", worktreePath: directory } },
      }
      const { layer } = yield* replay([response, response])
      yield* Effect.gen(function* () {
        const runtime = yield* Runtime
        const input = { worker: page.thread.threadId, runId: page.recentRuns[0]!.runId }
        expect(yield* runtime.status(input)).toMatchObject({
          assignedBranch: "assigned",
          currentBranch: "changed",
          headCommit: head,
        })
        yield* git("checkout", "--detach", head)
        expect(yield* runtime.status(input)).toMatchObject({
          assignedBranch: "assigned",
          currentBranch: null,
          headCommit: head,
        })
      }).pipe(Effect.provide(layer))
    }).pipe(Effect.scoped),
  )

  it.effect("fails before connecting when OAuth tokens are missing", () =>
    Effect.gen(function* () {
      const tokenDirectory = yield* temporaryDirectory
      const result = yield* Effect.gen(function* () {
        return yield* Runtime
      }).pipe(Effect.provide(T3RuntimeLayer({ ...config, tokenDirectory })), Effect.flip)
      expect(result).toBeInstanceOf(RuntimeError)
      expect(result.operation).toBe("connect")
      expect(result.message).toContain("Sign in to T3 with full access")
    }).pipe(Effect.scoped),
  )

  it.effect("does not read a parent checkout when the worktree metadata is missing", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const recorded = yield* fixture("run/11-messages")
      const page = yield* Schema.decodeUnknownEffect(StatusPage)(recorded.data)
      const { layer } = yield* replay([
        { ...recorded, data: { ...recorded.data, thread: { ...page.thread, worktreePath: directory } } },
      ])
      const failure = yield* Effect.gen(function* () {
        return yield* (yield* Runtime).status({
          worker: page.thread.threadId,
          runId: page.recentRuns[0]!.runId,
        })
      }).pipe(Effect.provide(layer), Effect.flip)
      expect(failure).toBeInstanceOf(RuntimeError)
      expect(failure.message).toContain("not a Git worktree root")
    }).pipe(Effect.scoped),
  )

  it.effect.each([
    "tool-error",
    "malformed",
    "unknown-status",
    "unknown-run",
    "stuck-page",
    "stuck-text",
  ] as const)("maps %s to RuntimeError", (mode) =>
    Effect.gen(function* () {
      const recorded = yield* fixture("run/11-messages")
      const page = yield* Schema.decodeUnknownEffect(StatusPage)(recorded.data)
      const modified = {
        ...recorded,
        isError: mode === "tool-error",
        data:
          mode === "malformed"
            ? { thread: false }
            : {
                ...recorded.data,
                ...(mode === "unknown-status"
                  ? { recentRuns: [{ ...page.recentRuns[0]!, status: "new-terminal-state" }] }
                  : {}),
                ...(mode === "stuck-page" ? { hasMore: true, nextPosition: null } : {}),
                ...(mode === "stuck-text"
                  ? { items: [{ ...page.items.at(-1)!, textTruncated: true, nextTextOffset: null }] }
                  : {}),
              },
      }
      const { layer } = yield* replay([modified])
      const failure = yield* Effect.gen(function* () {
        return yield* (yield* Runtime).status({
          worker: page.thread.threadId,
          runId: mode === "unknown-run" ? RunId.make("missing") : page.recentRuns[0]!.runId,
        })
      }).pipe(Effect.provide(layer), Effect.flip)
      expect(failure).toBeInstanceOf(RuntimeError)
      expect(failure.operation).toBe("status")
    }).pipe(Effect.scoped),
  )
})
