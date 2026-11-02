import { createServer, type IncomingMessage, type ServerResponse } from "node:http"
import { Server } from "@modelcontextprotocol/sdk/server/index.js"
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js"
import {
  CallToolRequestSchema,
  ErrorCode,
  ListToolsRequestSchema,
  McpError,
  type CallToolResult,
  type Tool,
} from "@modelcontextprotocol/sdk/types.js"
import { Effect, FiberSet, Layer, Schema } from "effect"
import { WorkerId } from "../core/domain/ids.ts"
import { Settings } from "../core/Settings.ts"
import { WorkerApi } from "../ports/WorkerApi.ts"

class McpServerError extends Schema.TaggedError<McpServerError>()("McpServerError", {
  cause: Schema.Defect(),
}) {}

const AskInput = Schema.Struct({ worker_id: WorkerId, question: Schema.NonEmptyString })
const ReportInput = Schema.Struct({ worker_id: WorkerId, text: Schema.NonEmptyString })

const tools: Array<Tool> = [
  {
    name: "ask",
    description: "Ask atta for context about your work.",
    inputSchema: {
      type: "object",
      properties: {
        worker_id: { type: "string", minLength: 1 },
        question: { type: "string", minLength: 1 },
      },
      required: ["worker_id", "question"],
      additionalProperties: false,
    },
    annotations: { readOnlyHint: true, openWorldHint: false },
  },
  {
    name: "report",
    description: "Tell atta when you finish work or get blocked.",
    inputSchema: {
      type: "object",
      properties: {
        worker_id: { type: "string", minLength: 1 },
        text: { type: "string", minLength: 1 },
      },
      required: ["worker_id", "text"],
      additionalProperties: false,
    },
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false },
  },
]

const textResult = (text: string, isError = false): CallToolResult => ({
  content: [{ type: "text", text }],
  ...(isError ? { isError: true } : {}),
})

/** Serves /mcp on IPv4 loopback for the lifetime of the layer's scope. */
export const McpServerLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    const { mcp } = yield* Settings
    const api = yield* WorkerApi
    const runTool = yield* FiberSet.makeRuntimePromise()
    const runRequest = yield* FiberSet.makeRuntime()

    const callTool = Effect.fn("McpServer.callTool")(
      function* (name: string, input: unknown) {
        if (name === "ask") {
          const args = yield* Schema.decodeUnknownEffect(AskInput)(input, { onExcessProperty: "error" })
          return textResult(yield* api.ask({ workerId: args.worker_id, question: args.question }))
        }
        const args = yield* Schema.decodeUnknownEffect(ReportInput)(input, { onExcessProperty: "error" })
        yield* api.report({ workerId: args.worker_id, text: args.text })
        return textResult("Report received.")
      },
      Effect.catchTag("WorkerApiError", (error) =>
        Effect.succeed(textResult(`${error.reason}: ${error.message}`, true)),
      ),
      Effect.catchTag("SchemaError", (error) =>
        Effect.succeed(textResult(`Invalid tool input: ${error.message}`, true)),
      ),
    )

    const handleRequest = Effect.fn("McpServer.handleRequest")(function* (
      request: IncomingMessage,
      response: ServerResponse,
    ) {
      if (request.url?.split("?", 1)[0] !== "/mcp") {
        response.writeHead(404).end()
        return
      }

      // Stateless transports cannot be shared, even between requests from the same client.
      const connection = yield* Effect.acquireRelease(
        Effect.sync(() => {
          const server = new Server({ name: "atta", version: "0.0.0" }, { capabilities: { tools: {} } })
          const hosts = [`127.0.0.1:${mcp.port}`, `localhost:${mcp.port}`]
          const transport = new StreamableHTTPServerTransport({
            enableJsonResponse: true,
            enableDnsRebindingProtection: true,
            allowedHosts: hosts,
            allowedOrigins: hosts.map((host) => `http://${host}`),
          })
          server.setRequestHandler(ListToolsRequestSchema, () => ({ tools }))
          server.setRequestHandler(CallToolRequestSchema, (call, { signal }) => {
            if (call.params.name !== "ask" && call.params.name !== "report") {
              throw new McpError(ErrorCode.InvalidParams, `Unknown tool: ${call.params.name}`)
            }
            return runTool(callTool(call.params.name, call.params.arguments), { signal })
          })
          return { server, transport }
        }),
        ({ server, transport }) =>
          Effect.promise(async () => {
            await server.close()
            await transport.close()
          }),
      )
      yield* Effect.tryPromise(() => connection.server.connect(connection.transport))
      yield* Effect.tryPromise(() => connection.transport.handleRequest(request, response))
    })

    yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: () =>
          new Promise<ReturnType<typeof createServer>>((resolve, reject) => {
            const http = createServer((request, response) => {
              runRequest(
                handleRequest(request, response).pipe(
                  Effect.scoped,
                  Effect.catch((error) =>
                    Effect.gen(function* () {
                      yield* Effect.logError("MCP request failed", error)
                      if (!response.headersSent) response.writeHead(500).end()
                    }),
                  ),
                  Effect.ensuring(
                    Effect.sync(() => {
                      if (!response.writableEnded) response.destroy()
                    }),
                  ),
                ),
              )
            })
            http.once("error", reject)
            http.listen(mcp.port, "127.0.0.1", () => {
              http.off("error", reject)
              resolve(http)
            })
          }),
        catch: (cause) => new McpServerError({ cause }),
      }),
      (http) =>
        Effect.callback<void>((resume) => {
          http.close(() => resume(Effect.void))
          http.closeAllConnections()
        }),
    )
  }),
)
