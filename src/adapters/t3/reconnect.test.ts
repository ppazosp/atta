import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js"
import { Server } from "@modelcontextprotocol/sdk/server/index.js"
import { CallToolRequestSchema } from "@modelcontextprotocol/sdk/types.js"
import { describe, expect, it } from "@effect/vitest"
import { Effect } from "effect"
import { RuntimeRef } from "../../core/domain/ids.ts"
import { Runtime } from "../../ports/Runtime.ts"
import { T3RuntimeLayer } from "./index.ts"
import { config, fixture } from "./test-support.ts"

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

describe("T3 restarts", () => {
  it.effect("reconnects once when T3 restarted, and answers the call on the new connection", () =>
    Effect.gen(function* () {
      const page = yield* fixture("run/10-activity-last-page")
      const servers: Array<Server> = []
      // Each connection gets its own server, like a T3 that restarted between calls.
      const transport = () => {
        const [client, server] = InMemoryTransport.createLinkedPair()
        const instance = new Server({ name: "t3", version: "1" }, { capabilities: { tools: {} } })
        instance.setRequestHandler(CallToolRequestSchema, () => ({
          content: [{ type: "text" as const, text: JSON.stringify(page.data) }],
          structuredContent: page.data,
        }))
        void instance.connect(server)
        servers.push(instance)
        return client
      }
      const layer = T3RuntimeLayer(config, { transport, oauthProvider })
      yield* Effect.gen(function* () {
        const runtime = yield* Runtime
        const worker = RuntimeRef.make("thread")
        yield* runtime.steps({ worker, afterPosition: 0 })
        // T3 goes away: the first connection's server closes.
        yield* Effect.promise(() => servers[0]!.close())
        const after = yield* runtime.steps({ worker, afterPosition: 0 })
        expect(after.nextPosition).toBe(0)
        expect(servers).toHaveLength(2)
      }).pipe(Effect.provide(layer))
    }).pipe(Effect.scoped),
  )
})
