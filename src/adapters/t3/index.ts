import { Client } from "@modelcontextprotocol/sdk/client/index.js"
import type { OAuthClientProvider } from "@modelcontextprotocol/sdk/client/auth.js"
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js"
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js"
import { McpError } from "@modelcontextprotocol/sdk/types.js"
import { Effect, Layer, Ref, Schema, Semaphore } from "effect"
import type { RuntimeRef } from "../../core/domain/ids.ts"
import type { RunOutcome, Step, StepKind, StepStatus } from "../../core/domain/worker.ts"
import { Runtime, RuntimeError } from "../../ports/Runtime.ts"
import { readWorktree } from "./git.ts"
import { fileOAuthProvider, signInRequired } from "./oauth.ts"
import {
  ActivityPage,
  Config,
  type Item,
  LaunchResult,
  SendResult,
  StatusPage,
  StopResult,
  ToolResult,
} from "./schemas.ts"

export { fileOAuthProvider, T3_REDIRECT_URL } from "./oauth.ts"
export type T3RuntimeConfig = typeof Config.Type

interface Options {
  /** A transport, or a function that makes a fresh one for each connection (tests use it to restart T3). */
  readonly transport?: Transport | (() => Transport)
  readonly oauthProvider?: OAuthClientProvider
}

const kindOf = (type: string): StepKind => {
  switch (type) {
    case "command_execution":
      return "command"
    case "file_change":
      return "file_change"
    case "assistant_message":
    case "user_message":
      return "message"
    case "reasoning":
      return "reasoning"
    case "checkpoint":
      return "checkpoint"
    default:
      return "other"
  }
}

const statusOf = (status: Item["status"]): StepStatus => {
  switch (status) {
    case "idle":
    case "pending":
    case "queued":
    case "preparing":
    case "starting":
      return "pending"
    case "running":
    case "waiting":
    case "in_progress":
      return "in_progress"
    case "completed":
      return "complete"
    default:
      return "error"
  }
}

const outcomeOf = (status: Item["status"]): RunOutcome | null => {
  switch (status) {
    case "completed":
      return "completed"
    case "interrupted":
      return "stopped"
    case "cancelled":
      return "interrupted"
    case "failed":
    case "error":
      return "failed"
    default:
      return null
  }
}

const planStatus = { pending: "pending", running: "in_progress", completed: "complete" } as const

/**
 * Codex's plan (its plan tool) arrives as one `todo_list` item per run, updated in place, whose text lists the
 * steps as `[status] text`. Each step becomes its own step here, with an ID stable across updates.
 */
const planStepsOf = (item: Item & { readonly runId: NonNullable<Item["runId"]> }): Array<Step> =>
  (item.text ?? "").split(/\r?\n/).flatMap((line, index) => {
    const match = /^\[(pending|running|completed)\]\s+(.+)$/.exec(line.trim())
    if (match === null) return []
    const [, status, text] = match as unknown as [string, keyof typeof planStatus, string]
    return [
      {
        id: `${item.itemId}#${index}`,
        position: item.position,
        runId: item.runId,
        kind: "plan_step" as const,
        status: planStatus[status],
        title: text.slice(0, 120),
        detail: null,
      },
    ]
  })

const stepsOf = (item: Item): Array<Step> => {
  if (item.runId === null) return []
  if (item.type === "todo_list") return planStepsOf({ ...item, runId: item.runId })
  const step = stepOf(item)
  return step === null ? [] : [step]
}

const stepOf = (item: Item): Step | null =>
  item.runId === null
    ? null
    : {
        id: item.itemId,
        position: item.position,
        runId: item.runId,
        kind: kindOf(item.type),
        status: statusOf(item.status),
        title: (item.title?.trim() || item.text?.split(/\r?\n/, 1)[0]?.trim() || item.type).slice(0, 120),
        detail: item.text || null,
      }

const describeCause = (cause: unknown) =>
  (cause instanceof Error ? cause.message : typeof cause === "string" ? cause : String(cause)).slice(0, 500)

/** baseBranch is explicit so launching never follows a worker's current branch. */
export const T3RuntimeLayer = (input: T3RuntimeConfig, options: Options = {}) =>
  Layer.effect(
    Runtime,
    Effect.gen(function* () {
      const config = yield* Schema.decodeUnknownEffect(Config)(input).pipe(
        Effect.mapError(
          () => new RuntimeError({ operation: "connect", message: "Invalid T3 runtime configuration." }),
        ),
      )
      const url = yield* Schema.decodeUnknownEffect(Schema.URLFromString)(config.url).pipe(
        Effect.filterOrFail(
          (endpoint) => endpoint.protocol === "http:" || endpoint.protocol === "https:",
          () => new RuntimeError({ operation: "connect", message: "T3 URL must use HTTP or HTTPS." }),
        ),
        Effect.mapError(
          () =>
            new RuntimeError({
              operation: "connect",
              message: "Invalid T3 MCP URL; expected HTTP or HTTPS.",
            }),
        ),
      )
      const provider = options.oauthProvider ?? fileOAuthProvider(config.tokenDirectory)
      const tokens = yield* Effect.tryPromise({
        try: async () => provider.tokens(),
        catch: () =>
          new RuntimeError({
            operation: "connect",
            message: `Cannot read T3 OAuth tokens in ${config.tokenDirectory}. Sign in to T3 again.`,
          }),
      })
      if (!tokens?.access_token) return yield* signInRequired(config.tokenDirectory)

      const makeTransport = (): Transport =>
        typeof options.transport === "function"
          ? options.transport()
          : (options.transport ?? new StreamableHTTPClientTransport(url, { authProvider: provider }))
      const open = Effect.tryPromise({
        try: async (signal) => {
          const client = new Client({ name: "atta", version: "0.0.0" })
          await client.connect(makeTransport(), { signal })
          return client
        },
        catch: () =>
          new RuntimeError({
            operation: "connect",
            message: "Cannot connect to T3 MCP. Check the endpoint and sign in with full access.",
          }),
      })
      const closeQuietly = (client: Client) => Effect.tryPromise(() => client.close()).pipe(Effect.ignore)
      const connection = yield* Ref.make(yield* open)
      yield* Effect.addFinalizer(() => Ref.get(connection).pipe(Effect.flatMap(closeQuietly)))
      const reconnecting = yield* Semaphore.make(1)
      /**
       * T3 can restart under atta (an update, a crash, launchd). Its old session is then gone, so a call that
       * fails on the connection opens a new one once. Calls racing here share a single reconnect.
       */
      const reconnect = (failed: Client) =>
        reconnecting.withPermits(1)(
          Effect.gen(function* () {
            const current = yield* Ref.get(connection)
            if (current !== failed) return current
            yield* closeQuietly(failed)
            const fresh = yield* open
            yield* Ref.set(connection, fresh)
            yield* Effect.logInfo("reconnected to T3")
            return fresh
          }),
        )

      const call = Effect.fn("T3Runtime.call")(function* <A>(
        operation: string,
        name: string,
        args: Record<string, unknown>,
        schema: Schema.Codec<A, unknown>,
      ) {
        // Keep the cause: "T3 failed" alone makes a production failure impossible to diagnose.
        const failure = (cause?: unknown) =>
          new RuntimeError({
            operation,
            message: `T3 ${name} failed or returned an invalid response${cause === undefined ? "" : `: ${describeCause(cause)}`}`,
          })
        const attempt = (client: Client) =>
          Effect.tryPromise({
            try: (signal) => client.callTool({ name, arguments: args }, undefined, { signal }),
            catch: (cause) => cause,
          })
        const client = yield* Ref.get(connection)
        const raw = yield* attempt(client).pipe(
          // An error T3 sent back is an answer; any other failure means the connection is gone.
          Effect.catch((cause) =>
            cause instanceof McpError
              ? Effect.fail(failure(cause))
              : reconnect(client).pipe(Effect.flatMap(attempt), Effect.mapError(failure)),
          ),
        )
        const result = yield* Schema.decodeUnknownEffect(ToolResult)(raw).pipe(Effect.mapError(failure))
        if (result.isError) {
          return yield* failure(result.content?.find((block) => block.type === "text")?.text ?? "tool error")
        }
        if (result.structuredContent !== undefined) {
          return yield* Schema.decodeUnknownEffect(schema)(result.structuredContent).pipe(
            Effect.mapError(failure),
          )
        }
        const text = result.content?.find((block) => block.type === "text")?.text
        return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(schema))(text).pipe(
          Effect.mapError(failure),
        )
      })

      const readMessage = Effect.fn("T3Runtime.readMessage")(function* (worker: RuntimeRef, item: Item) {
        let text = item.text ?? ""
        let part = item
        let offset = 0
        while (part.textTruncated) {
          const nextOffset = part.nextTextOffset
          if (nextOffset == null || nextOffset <= offset)
            return yield* new RuntimeError({
              operation: "status",
              message: "T3 message text paging did not advance.",
            })
          const page = yield* call(
            "status",
            "t3_thread_read",
            {
              threadId: worker,
              view: "messages",
              itemId: item.itemId,
              textOffset: nextOffset,
              maxCharsPerItem: 2000,
            },
            ActivityPage,
          )
          const chunk = page.items.find(
            (candidate) => candidate.itemId === item.itemId && candidate.runId === item.runId,
          )
          if (chunk === undefined)
            return yield* new RuntimeError({
              operation: "status",
              message: "T3 did not return the requested message text.",
            })
          text += chunk.text ?? ""
          offset = nextOffset
          part = chunk
        }
        return item.text === null ? null : text
      })

      return Runtime.of({
        launch: Effect.fn("T3Runtime.launch")(function* ({ repo, brief, operationId }) {
          const project = config.repos.find((configured) => configured.name === repo)
          if (project === undefined)
            return yield* new RuntimeError({
              operation: "launch",
              message: `No T3 project configured for repository ${repo}.`,
            })
          const result = yield* call(
            "launch",
            "t3_thread_launch",
            {
              projectId: project.t3ProjectId,
              title: brief.title,
              message: brief.text,
              clientRequestId: operationId,
              workspaceStrategy: { type: "worktree", baseRef: project.baseBranch },
              modelSelection: {
                instanceId: config.instanceId,
                model: config.model,
                options: [{ id: "reasoningEffort", value: config.effort }],
              },
              runtimeMode: "full-access",
            },
            LaunchResult,
          )
          return {
            runtimeRef: result.threadId,
            runId: result.runId,
            branch: result.branch ?? null,
            worktreePath: result.worktreePath ?? null,
          }
        }),
        send: Effect.fn("T3Runtime.send")(function* ({ worker, text, mode, operationId }) {
          return yield* call(
            "send",
            "t3_thread_send",
            { threadId: worker, message: text, mode, clientRequestId: operationId },
            SendResult,
          )
        }),
        stop: Effect.fn("T3Runtime.stop")(function* ({ worker, operationId }) {
          yield* call(
            "stop",
            "t3_thread_interrupt",
            { threadId: worker, stop: true, clientRequestId: operationId },
            StopResult,
          )
        }),
        steps: Effect.fn("T3Runtime.steps")(function* ({ worker, afterPosition }) {
          const page = yield* call(
            "steps",
            "t3_thread_read",
            {
              threadId: worker,
              view: "activity",
              afterPosition,
              limit: 50,
              maxCharsPerItem: 400,
            },
            ActivityPage,
          )
          const steps = page.items.flatMap(stepsOf)
          const unfinished = steps.find((step) => step.status === "pending" || step.status === "in_progress")
          const nextPosition =
            unfinished === undefined ? (page.nextPosition ?? afterPosition) : unfinished.position - 1
          return { steps, nextPosition: Math.max(afterPosition, nextPosition) }
        }),
        status: Effect.fn("T3Runtime.status")(function* ({ worker, runId }) {
          let position: number | undefined
          let lastMessage: Item | undefined
          let outcome: RunOutcome | null = null
          let assignedBranch: string | null = null
          let worktreePath: string | null = null
          let found = false
          while (true) {
            const page = yield* call(
              "status",
              "t3_thread_read",
              {
                threadId: worker,
                view: "messages",
                limit: 50,
                runLimit: 50,
                maxCharsPerItem: 2000,
                ...(position === undefined ? {} : { afterPosition: position }),
              },
              StatusPage,
            )
            if (page.thread.threadId !== worker)
              return yield* new RuntimeError({
                operation: "status",
                message: "T3 returned a different thread.",
              })
            const run = page.recentRuns.find((candidate) => candidate.runId === runId)
            if (run !== undefined) {
              found = true
              outcome = outcomeOf(run.status)
            }
            assignedBranch = page.thread.branch
            worktreePath = page.thread.worktreePath
            for (const item of page.items) {
              if (item.runId === runId && item.type === "assistant_message") lastMessage = item
            }
            if (!page.hasMore) break
            if (page.nextPosition === null || (position !== undefined && page.nextPosition <= position)) {
              return yield* new RuntimeError({
                operation: "status",
                message: "T3 message paging did not advance.",
              })
            }
            position = page.nextPosition
          }
          if (!found)
            return yield* new RuntimeError({
              operation: "status",
              message: `T3 did not report the requested run ${runId} in recentRuns.`,
            })
          const git = yield* readWorktree(worktreePath)
          const finalMessage = lastMessage === undefined ? null : yield* readMessage(worker, lastMessage)
          return {
            runId,
            state: outcome === null ? "running" : "finished",
            outcome,
            finalMessage,
            assignedBranch,
            ...git,
          }
        }),
      })
    }),
  )
