import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises"
import { join } from "node:path"
import { expect } from "@effect/vitest"
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js"
import { Server } from "@modelcontextprotocol/sdk/server/index.js"
import { CallToolRequestSchema } from "@modelcontextprotocol/sdk/types.js"
import { Effect, Schema } from "effect"
import { T3RuntimeLayer, type T3RuntimeConfig } from "./index.ts"

const Fixture = Schema.Struct({
  tool: Schema.String,
  args: Schema.Record(Schema.String, Schema.Unknown),
  isError: Schema.Boolean,
  data: Schema.Record(Schema.String, Schema.Unknown),
})
type Fixture = typeof Fixture.Type

export const config: T3RuntimeConfig = {
  url: "http://127.0.0.1:3999/mcp",
  tokenDirectory: "./data/t3-test-tokens",
  repos: [{ name: "sandbox-repo", t3ProjectId: "b7bbe39b-2fbc-44d8-8238-c8527731c5b4", baseBranch: "main" }],
  instanceId: "codex",
  model: "gpt-6.1-sol",
  effort: "low",
}

export const fixture = Effect.fn("T3Test.fixture")(function* (name: string) {
  const text = yield* Effect.tryPromise(() =>
    readFile(new URL(`../../../test/fixtures/t3/${name}.json`, import.meta.url), "utf8"),
  )
  return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Fixture))(text)
})

export const temporaryDirectory = Effect.acquireRelease(
  Effect.tryPromise(async () => {
    const root = join(process.cwd(), "data")
    await mkdir(root, { recursive: true })
    return mkdtemp(join(root, "t3-test-"))
  }),
  (directory) => Effect.tryPromise(() => rm(directory, { recursive: true, force: true })).pipe(Effect.orDie),
)

export const replay = Effect.fn("T3Test.replay")(function* (
  fixtures: ReadonlyArray<Fixture>,
  options: { readonly textOnly?: boolean; readonly runtimeConfig?: T3RuntimeConfig } = {},
) {
  const calls: Array<{ name: string; arguments?: Record<string, unknown> | undefined }> = []
  const [transport, serverTransport] = InMemoryTransport.createLinkedPair()
  const server = yield* Effect.acquireRelease(
    Effect.sync(() => {
      const instance = new Server({ name: "fixture-t3", version: "1" }, { capabilities: { tools: {} } })
      instance.setRequestHandler(CallToolRequestSchema, (request) => {
        const entry = fixtures[calls.length]
        calls.push(request.params)
        if (entry === undefined) throw new Error("Unexpected T3 request")
        expect(request.params.name).toBe(entry.tool)
        return {
          isError: entry.isError,
          content: [{ type: "text" as const, text: JSON.stringify(entry.data) }],
          ...(options.textOnly ? {} : { structuredContent: entry.data }),
        }
      })
      return instance
    }),
    (instance) => Effect.tryPromise(() => instance.close()).pipe(Effect.orDie),
  )
  yield* Effect.tryPromise(() => server.connect(serverTransport))
  const oauthProvider = {
    redirectUrl: undefined,
    clientMetadata: { redirect_uris: [] },
    clientInformation: () => undefined,
    tokens: () => ({ access_token: "fixture-token", token_type: "Bearer" }),
    saveTokens: () => {},
    redirectToAuthorization: () => {},
    saveCodeVerifier: () => {},
    codeVerifier: () => "unused",
  }
  const layer = T3RuntimeLayer(options.runtimeConfig ?? config, { transport, oauthProvider })
  return { layer, calls }
})
