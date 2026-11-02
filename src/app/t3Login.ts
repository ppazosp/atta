import { createServer } from "node:http"
import { Client } from "@modelcontextprotocol/sdk/client/index.js"
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js"
import { UnauthorizedError } from "@modelcontextprotocol/sdk/client/auth.js"
import { Console, Effect } from "effect"
import { fileOAuthProvider } from "../adapters/t3/index.ts"

const CALLBACK_PORT = 8765
const REDIRECT = `http://localhost:${CALLBACK_PORT}/callback`

/** Waits for T3's OAuth redirect on localhost and returns the code. */
const waitForCode = () =>
  new Promise<string>((resolve, reject) => {
    const server = createServer((req, res) => {
      const url = new URL(req.url ?? "/", REDIRECT)
      if (url.pathname !== "/callback") return void res.writeHead(404).end()
      const code = url.searchParams.get("code")
      res
        .writeHead(200, { "content-type": "text/plain" })
        .end(code ? "atta is signed in to T3. You can close this tab." : "Sign-in failed.")
      server.close()
      if (code) resolve(code)
      else reject(new Error(url.searchParams.get("error") ?? "no code"))
    })
    server.listen(CALLBACK_PORT, "127.0.0.1")
  })

/**
 * Signs atta in to T3's MCP endpoint, interactively, and saves the tokens where the T3 adapter reads them.
 * Approve with full access in T3: atta launches and steers full-access workers.
 */
export const t3Login = (url: string, tokenDirectory: string) =>
  Effect.gen(function* () {
    let verifier = ""
    const saved = fileOAuthProvider(tokenDirectory)
    const provider = {
      ...saved,
      redirectUrl: REDIRECT,
      clientMetadata: {
        client_name: "atta",
        redirect_uris: [REDIRECT],
        grant_types: ["authorization_code", "refresh_token"],
        response_types: ["code"],
        token_endpoint_auth_method: "none",
        scope: "orchestration:read orchestration:operate",
      },
      redirectToAuthorization: async (authorizationUrl: URL) => {
        console.log(`Open this link and approve atta with full access:\n${authorizationUrl.toString()}`)
      },
      saveCodeVerifier: async (value: string) => {
        verifier = value
      },
      codeVerifier: async () => verifier,
    }
    const connect = async () => {
      const transport = new StreamableHTTPClientTransport(new URL(url), { authProvider: provider })
      const client = new Client({ name: "atta", version: "0.0.0" })
      try {
        await client.connect(transport)
        return client
      } catch (error) {
        if (!(error instanceof UnauthorizedError)) throw error
        await transport.finishAuth(await waitForCode())
        const again = new Client({ name: "atta", version: "0.0.0" })
        await again.connect(new StreamableHTTPClientTransport(new URL(url), { authProvider: provider }))
        return again
      }
    }
    const tools = yield* Effect.tryPromise(async () => {
      const client = await connect()
      const listed = await client.listTools()
      await client.close()
      return listed.tools.length
    })
    yield* Console.log(`Signed in to T3 at ${url}: ${tools} tools available.`)
  })
