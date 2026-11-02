import { describe, expect, it } from "@effect/vitest"
import { LATEST_PROTOCOL_VERSION } from "@modelcontextprotocol/sdk/types.js"
import { Effect, Schema } from "effect"
import { vi } from "vitest"
import { OperationId } from "../../core/domain/ids.ts"
import { Runtime, RuntimeError } from "../../ports/Runtime.ts"
import { fileOAuthProvider, T3RuntimeLayer } from "./index.ts"
import { config, fixture, temporaryDirectory } from "./test-support.ts"

const Request = Schema.fromJsonString(
  Schema.Struct({
    method: Schema.String,
    id: Schema.optionalKey(Schema.Union([Schema.String, Schema.Number])),
  }),
)

describe("T3 Streamable HTTP client", () => {
  it.effect("uses the configured URL and saved OAuth token over Streamable HTTP", () =>
    Effect.gen(function* () {
      const tokenDirectory = yield* temporaryDirectory
      const launch = yield* fixture("run/02-launch")
      yield* Effect.tryPromise(async () =>
        fileOAuthProvider(tokenDirectory).saveTokens({
          access_token: "fixture-http-token",
          token_type: "Bearer",
        }),
      )
      const requests: Array<{ url: string; authorization: string | null; method: string }> = []
      yield* Effect.acquireRelease(
        Effect.sync(() =>
          vi.spyOn(globalThis, "fetch").mockImplementation(async (url, init) => {
            const authorization = new Headers(init?.headers).get("authorization")
            requests.push({ url: String(url), authorization, method: init?.method ?? "GET" })
            if (init?.method === "GET") return new Response(null, { status: 405 })
            const message = Schema.decodeUnknownSync(Request)(init?.body)
            if (message.id === undefined) return new Response(null, { status: 202 })
            const result =
              message.method === "initialize"
                ? {
                    protocolVersion: LATEST_PROTOCOL_VERSION,
                    capabilities: { tools: {} },
                    serverInfo: { name: "fixture-t3", version: "1" },
                  }
                : { content: [{ type: "text", text: JSON.stringify(launch.data) }] }
            return new Response(JSON.stringify({ jsonrpc: "2.0", id: message.id, result }), {
              headers: { "content-type": "application/json" },
            })
          }),
        ),
        (mock) => Effect.sync(() => mock.mockRestore()),
      )
      const result = yield* Effect.gen(function* () {
        return yield* (yield* Runtime).launch({
          repo: "sandbox-repo",
          brief: { repo: "sandbox-repo", title: "fixture", text: "fixture" },
          operationId: OperationId.make("fixture-http-launch"),
        })
      }).pipe(Effect.provide(T3RuntimeLayer({ ...config, tokenDirectory })))
      expect(result.runtimeRef).toBe(launch.data.threadId)
      expect(requests.length).toBeGreaterThanOrEqual(3)
      expect(
        requests.every(
          (request) => request.url === config.url && request.authorization === "Bearer fixture-http-token",
        ),
      ).toBe(true)
    }).pipe(Effect.scoped),
  )

  it.effect("maps transport connection failures to RuntimeError", () =>
    Effect.gen(function* () {
      const tokenDirectory = yield* temporaryDirectory
      yield* Effect.tryPromise(async () =>
        fileOAuthProvider(tokenDirectory).saveTokens({ access_token: "fixture-token", token_type: "Bearer" }),
      )
      yield* Effect.acquireRelease(
        Effect.sync(() => vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("fixture-offline"))),
        (mock) => Effect.sync(() => mock.mockRestore()),
      )
      const failure = yield* Effect.gen(function* () {
        return yield* Runtime
      }).pipe(Effect.provide(T3RuntimeLayer({ ...config, tokenDirectory })), Effect.flip)
      expect(failure).toBeInstanceOf(RuntimeError)
      expect(failure.operation).toBe("connect")
    }).pipe(Effect.scoped),
  )
})
