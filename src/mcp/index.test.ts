import { createServer, request, type IncomingHttpHeaders, type Server as HttpServer } from "node:http"
import { describe, expect, it } from "@effect/vitest"
import { Client } from "@modelcontextprotocol/sdk/client/index.js"
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js"
import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js"
import { Deferred, Effect, Exit, Fiber, Layer, Ref, Schema, Scope } from "effect"
import example from "../../atta.config.example.ts"
import { AttaConfig } from "../core/domain/config.ts"
import { Settings } from "../core/Settings.ts"
import { WorkerApi, WorkerApiError } from "../ports/WorkerApi.ts"
import { FakeWorkerApiControl, makeFakeWorkerApiLayer } from "../../test/fakes/FakeWorkerApi.ts"
import { McpServerLayer } from "./index.ts"

const closeHttp = (server: HttpServer) =>
  Effect.callback<void>((resume) => {
    server.close(() => resume(Effect.void))
    server.closeAllConnections()
  })

const listen = (server: HttpServer, port: number) =>
  Effect.tryPromise(
    () =>
      new Promise<void>((resolve, reject) => {
        server.once("error", reject)
        server.listen(port, "127.0.0.1", () => {
          server.off("error", reject)
          resolve()
        })
      }),
  )

const freePort = Effect.gen(function* () {
  const server = yield* Effect.acquireRelease(
    Effect.sync(() => createServer()),
    closeHttp,
  )
  yield* listen(server, 0)
  return (yield* Schema.decodeUnknownEffect(Schema.Struct({ port: Schema.Int }))(server.address())).port
}).pipe(Effect.scoped)

const startServer = Effect.fn("McpTest.startServer")(function* (port?: number) {
  const selectedPort = port ?? (yield* freePort)
  const config = yield* Schema.decodeUnknownEffect(AttaConfig)({ ...example, mcp: { port: selectedPort } })
  yield* Layer.build(McpServerLayer.pipe(Layer.provide(Layer.succeed(Settings)(config))))
  return selectedPort
})

const connect = Effect.fn("McpTest.connect")(function* (port: number) {
  const client = yield* Effect.acquireRelease(
    Effect.sync(() => new Client({ name: "atta-test", version: "1" })),
    (connection) => Effect.promise(() => connection.close()),
  )
  yield* Effect.tryPromise((signal) =>
    client.connect(new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`)), { signal }),
  )
  return client
})

const call = (client: Client, name: string, args?: Record<string, unknown>) =>
  Effect.tryPromise((signal) =>
    client.callTool({ name, ...(args === undefined ? {} : { arguments: args }) }, undefined, { signal }),
  )

const initialize = {
  jsonrpc: "2.0",
  id: 1,
  method: "initialize",
  params: {
    protocolVersion: "2025-11-25",
    capabilities: {},
    clientInfo: { name: "atta-test", version: "1" },
  },
}

const httpRequest = Effect.fn("McpTest.httpRequest")(
  (
    port: number,
    options: {
      readonly headers?: Record<string, string>
      readonly path?: string
      readonly method?: string
      readonly body?: unknown
    } = {},
  ) =>
    Effect.tryPromise(
      () =>
        new Promise<{
          readonly status: number | undefined
          readonly headers: IncomingHttpHeaders
          readonly body: string
        }>((resolve, reject) => {
          const req = request(
            {
              host: "127.0.0.1",
              port,
              path: options.path ?? "/mcp",
              method: options.method ?? "POST",
              agent: false,
              headers: {
                accept: "application/json, text/event-stream",
                "content-type": "application/json",
                ...options.headers,
              },
            },
            (response) => {
              let body = ""
              response.setEncoding("utf8")
              response.on("data", (chunk: string) => {
                body += chunk
              })
              response.on("error", reject)
              response.on("end", () =>
                resolve({ status: response.statusCode, headers: response.headers, body }),
              )
            },
          )
          req.on("error", reject)
          req.end(JSON.stringify(options.body ?? initialize))
        }),
    ),
)

const fake = makeFakeWorkerApiLayer("Use the sandbox repository.")

describe("worker MCP server", () => {
  it.effect("lists exactly ask, report, say and ask_approval with their JSON Schema inputs", () =>
    Effect.gen(function* () {
      const client = yield* connect(yield* startServer())
      const listed = yield* Effect.tryPromise(() => client.listTools())
      expect(listed.tools.map((tool) => tool.name)).toEqual(["ask", "report", "say", "ask_approval"])
      expect(listed.tools.map((tool) => tool.inputSchema)).toEqual([
        {
          type: "object",
          properties: {
            worker_id: { type: "string", minLength: 1 },
            question: { type: "string", minLength: 1 },
          },
          required: ["worker_id", "question"],
          additionalProperties: false,
        },
        {
          type: "object",
          properties: { worker_id: { type: "string", minLength: 1 }, text: { type: "string", minLength: 1 } },
          required: ["worker_id", "text"],
          additionalProperties: false,
        },
        {
          type: "object",
          properties: { worker_id: { type: "string", minLength: 1 }, text: { type: "string", minLength: 1 } },
          required: ["worker_id", "text"],
          additionalProperties: false,
        },
        {
          type: "object",
          properties: {
            worker_id: { type: "string", minLength: 1 },
            text: { type: "string", minLength: 1, description: "The full request, in Markdown." },
            images: {
              type: "array",
              maxItems: 4,
              items: { type: "string", minLength: 1 },
              description: "Absolute paths of PNG, JPEG or GIF files in your worktree, shown under the text.",
            },
          },
          required: ["worker_id", "text"],
          additionalProperties: false,
        },
      ])
    }).pipe(Effect.scoped, Effect.provide(fake)),
  )

  it.effect("passes ask, report, say and ask_approval to the fake WorkerApi and returns text", () =>
    Effect.gen(function* () {
      const client = yield* connect(yield* startServer())
      expect(yield* call(client, "ask", { worker_id: "worker-1", question: "Which repository?" })).toEqual({
        content: [{ type: "text", text: "Use the sandbox repository." }],
      })
      expect(yield* call(client, "report", { worker_id: "worker-1", text: "Finished the work." })).toEqual({
        content: [{ type: "text", text: "Report received." }],
      })
      expect(yield* call(client, "say", { worker_id: "worker-1", text: "Yes, using pnpm now." })).toEqual({
        content: [{ type: "text", text: "Posted in your thread." }],
      })
      expect(
        yield* call(client, "ask_approval", {
          worker_id: "worker-1",
          text: "## Plan\nOption A.",
          images: ["/work/chart.png"],
        }),
      ).toEqual({
        content: [
          {
            type: "text",
            text: "Posted request ask:worker-1 in your thread. End your run now: the user's answer starts your next one.",
          },
        ],
      })
      expect(yield* call(client, "ask_approval", { worker_id: "worker-1", text: "No images." })).toEqual({
        content: [
          {
            type: "text",
            text: "Posted request ask:worker-1 in your thread. End your run now: the user's answer starts your next one.",
          },
        ],
      })
      expect(yield* (yield* FakeWorkerApiControl).calls).toEqual([
        { _tag: "ask", workerId: "worker-1", question: "Which repository?" },
        { _tag: "report", workerId: "worker-1", text: "Finished the work." },
        { _tag: "say", workerId: "worker-1", text: "Yes, using pnpm now." },
        {
          _tag: "askApproval",
          workerId: "worker-1",
          text: "## Plan\nOption A.",
          images: ["/work/chart.png"],
        },
        { _tag: "askApproval", workerId: "worker-1", text: "No images.", images: [] },
      ])
    }).pipe(Effect.scoped, Effect.provide(fake)),
  )

  it.effect("serves concurrent clients with overlapping request IDs", () =>
    Effect.gen(function* () {
      const port = yield* startServer()
      const first = yield* connect(port)
      const second = yield* connect(port)
      const results = yield* Effect.all(
        [
          call(first, "ask", { worker_id: "worker-1", question: "First?" }),
          call(second, "ask", { worker_id: "worker-2", question: "Second?" }),
        ],
        { concurrency: "unbounded" },
      )
      expect(results.every((result) => result.isError !== true)).toBe(true)
      expect(yield* (yield* FakeWorkerApiControl).calls).toHaveLength(2)
    }).pipe(Effect.scoped, Effect.provide(fake)),
  )

  it.effect.each(["ask", "report", "say", "ask_approval"])(
    "turns an unknown worker into a %s tool error",
    (name) =>
      Effect.gen(function* () {
        const client = yield* connect(yield* startServer())
        const args = name === "ask" ? { question: "Context?" } : { text: "Done." }
        expect(yield* call(client, name, { worker_id: "unknown-worker", ...args })).toEqual({
          isError: true,
          content: [{ type: "text", text: "unknown_worker: no worker unknown-worker" }],
        })
        expect(yield* (yield* FakeWorkerApiControl).calls).toEqual([])
      }).pipe(Effect.scoped, Effect.provide(fake)),
  )

  it.effect.each(["worker_retired", "failed"] as const)("preserves a %s WorkerApi error", (reason) =>
    Effect.gen(function* () {
      const client = yield* connect(yield* startServer())
      for (const name of ["ask", "report", "say", "ask_approval"]) {
        const args = name === "ask" ? { question: "Context?" } : { text: "Done." }
        expect(yield* call(client, name, { worker_id: "worker-1", ...args })).toEqual({
          isError: true,
          content: [
            {
              type: "text",
              text: `${reason}: ${reason === "worker_retired" ? "worker-1 is retired" : "storage unavailable"}`,
            },
          ],
        })
      }
    }).pipe(
      Effect.scoped,
      Effect.provideService(
        WorkerApi,
        WorkerApi.of({
          ask: () =>
            Effect.fail(
              new WorkerApiError({
                reason,
                message: reason === "worker_retired" ? "worker-1 is retired" : "storage unavailable",
              }),
            ),
          report: () =>
            Effect.fail(
              new WorkerApiError({
                reason,
                message: reason === "worker_retired" ? "worker-1 is retired" : "storage unavailable",
              }),
            ),
          say: () =>
            Effect.fail(
              new WorkerApiError({
                reason,
                message: reason === "worker_retired" ? "worker-1 is retired" : "storage unavailable",
              }),
            ),
          askApproval: () =>
            Effect.fail(
              new WorkerApiError({
                reason,
                message: reason === "worker_retired" ? "worker-1 is retired" : "storage unavailable",
              }),
            ),
        }),
      ),
    ),
  )

  const invalidInputs: Array<Record<string, unknown> | undefined> = [
    undefined,
    {},
    { worker_id: "worker-1" },
    { worker_id: 42, question: "Context?", text: "Done." },
    { worker_id: "", question: "Context?", text: "Done." },
    { worker_id: "worker-1", question: null },
    { worker_id: "worker-1", text: false },
    { worker_id: "worker-1", question: "", text: "" },
  ]
  for (const name of ["ask", "report", "say", "ask_approval"]) {
    it.effect.each(invalidInputs)(`rejects invalid ${name} input %# before calling WorkerApi`, (input) =>
      Effect.gen(function* () {
        const client = yield* connect(yield* startServer())
        const result = yield* call(client, name, input)
        expect(result.isError).toBe(true)
        expect(result.content).toEqual([
          { type: "text", text: expect.stringContaining("Invalid tool input:") },
        ])
        expect(yield* (yield* FakeWorkerApiControl).calls).toEqual([])
      }).pipe(Effect.scoped, Effect.provide(fake)),
    )
  }

  it.effect("rejects unknown tools as invalid MCP requests", () =>
    Effect.gen(function* () {
      const client = yield* connect(yield* startServer())
      const failure = yield* call(client, "unknown", {}).pipe(Effect.flip)
      expect(failure.cause).toBeInstanceOf(McpError)
      expect(failure.cause).toMatchObject({ code: ErrorCode.InvalidParams })
      expect(yield* (yield* FakeWorkerApiControl).calls).toEqual([])
    }).pipe(Effect.scoped, Effect.provide(fake)),
  )

  it.effect.each(["127.0.0.1", "localhost"])(
    "allows the %s Host and Origin without creating a session",
    (host) =>
      Effect.gen(function* () {
        const port = yield* startServer()
        const response = yield* httpRequest(port, {
          headers: { host: `${host}:${port}`, origin: `http://${host}:${port}` },
        })
        expect(response.status).toBe(200)
        expect(response.headers["mcp-session-id"]).toBeUndefined()
        expect(response.body).toContain('"serverInfo":{"name":"atta"')
      }).pipe(Effect.scoped, Effect.provide(fake)),
  )

  it.effect.each(["attacker.invalid", "localhost.attacker.invalid", "192.0.2.1"])(
    "refuses a non-localhost Host: %s",
    (host) =>
      Effect.gen(function* () {
        const port = yield* startServer()
        const response = yield* httpRequest(port, { headers: { host: `${host}:${port}` } })
        expect(response.status).toBe(403)
        expect(response.body).toContain("Invalid Host header")
        expect(yield* (yield* FakeWorkerApiControl).calls).toEqual([])
      }).pipe(Effect.scoped, Effect.provide(fake)),
  )

  it.effect("refuses a foreign Origin even with a localhost Host", () =>
    Effect.gen(function* () {
      const port = yield* startServer()
      const response = yield* httpRequest(port, { headers: { origin: "https://attacker.invalid" } })
      expect(response.status).toBe(403)
      expect(response.body).toContain("Invalid Origin header")
    }).pipe(Effect.scoped, Effect.provide(fake)),
  )

  it.effect("keeps other paths outside the MCP endpoint", () =>
    Effect.gen(function* () {
      const port = yield* startServer()
      expect((yield* httpRequest(port, { path: "/elsewhere" })).status).toBe(404)
    }).pipe(Effect.scoped, Effect.provide(fake)),
  )

  it.effect("closes the listener with its scope and releases the configured port", () =>
    Effect.gen(function* () {
      const port = yield* Effect.gen(function* () {
        const startedPort = yield* startServer()
        const client = yield* connect(startedPort)
        yield* Effect.tryPromise(() => client.listTools())
        return startedPort
      }).pipe(Effect.scoped)
      expect((yield* httpRequest(port).pipe(Effect.flip)).cause).toMatchObject({ code: "ECONNREFUSED" })
      const replacement = yield* Effect.acquireRelease(
        Effect.sync(() => createServer()),
        closeHttp,
      )
      yield* listen(replacement, port)
      expect(replacement.address()).toMatchObject({ address: "127.0.0.1", port })
    }).pipe(Effect.scoped, Effect.provide(fake)),
  )

  it.effect("interrupts a running WorkerApi call when the server scope closes", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>()
      const interrupted = yield* Ref.make(false)
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const port = yield* startServer().pipe(
        Effect.provideService(Scope.Scope, scope),
        Effect.provideService(
          WorkerApi,
          WorkerApi.of({
            ask: () =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.onInterrupt(() => Ref.set(interrupted, true)),
              ),
            report: () => Effect.void,
            say: () => Effect.void,
            askApproval: () => Effect.succeed("ask:worker-1"),
          }),
        ),
      )
      const client = yield* connect(port)
      const pending = yield* call(client, "ask", { worker_id: "worker-1", question: "Context?" }).pipe(
        Effect.exit,
        Effect.forkScoped,
      )
      yield* Deferred.await(entered)
      yield* Scope.close(scope, Exit.void)
      expect(yield* Ref.get(interrupted)).toBe(true)
      expect(Exit.isFailure(yield* Fiber.join(pending))).toBe(true)
      expect((yield* httpRequest(port).pipe(Effect.flip)).cause).toMatchObject({ code: "ECONNREFUSED" })
    }).pipe(Effect.scoped),
  )

  it.effect("fails to start on an occupied port without closing the existing listener", () =>
    Effect.gen(function* () {
      const existing = yield* Effect.acquireRelease(
        Effect.sync(() => createServer()),
        closeHttp,
      )
      yield* listen(existing, 0)
      const { port } = yield* Schema.decodeUnknownEffect(Schema.Struct({ port: Schema.Int }))(
        existing.address(),
      )
      const failure = yield* startServer(port).pipe(Effect.scoped, Effect.flip)
      expect(failure).toMatchObject({ _tag: "McpServerError", cause: { code: "EADDRINUSE" } })
      expect(existing.listening).toBe(true)
    }).pipe(Effect.scoped, Effect.provide(fake)),
  )
})
