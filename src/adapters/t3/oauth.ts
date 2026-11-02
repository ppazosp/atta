import { randomUUID } from "node:crypto"
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises"
import { join } from "node:path"
import type { OAuthClientProvider } from "@modelcontextprotocol/sdk/client/auth.js"
import { Effect, Schema } from "effect"
import { RuntimeError } from "../../ports/Runtime.ts"

const Tokens = Schema.Struct({
  access_token: Schema.NonEmptyString,
  token_type: Schema.NonEmptyString,
  refresh_token: Schema.optionalKey(Schema.String),
  expires_in: Schema.optionalKey(Schema.Number),
  scope: Schema.optionalKey(Schema.String),
  id_token: Schema.optionalKey(Schema.String),
  issuer: Schema.optionalKey(Schema.String),
})
const ClientInformation = Schema.Struct({
  client_id: Schema.NonEmptyString,
  client_secret: Schema.optionalKey(Schema.String),
  client_id_issued_at: Schema.optionalKey(Schema.Number),
  client_secret_expires_at: Schema.optionalKey(Schema.Number),
  issuer: Schema.optionalKey(Schema.String),
})

const FileError = Schema.Struct({ code: Schema.String })

/** Where T3 sends the browser back after `atta login t3`. */
export const T3_REDIRECT_URL = "http://localhost:8765/callback"

export const signInRequired = (directory: string) =>
  new RuntimeError({
    operation: "connect",
    message: `No usable T3 OAuth tokens in ${directory}. Sign in to T3 with full access before starting atta.`,
  })

/** tokens.json and client.json contain the SDK's OAuth tokens and client registration. */
export const fileOAuthProvider = (directory: string): OAuthClientProvider => {
  const read = Effect.fn("T3OAuth.read")(function* <A>(filename: string, schema: Schema.Codec<A>) {
    const contents = yield* Effect.tryPromise(() => readFile(join(directory, filename), "utf8")).pipe(
      Effect.catch((error) =>
        Schema.decodeUnknownEffect(FileError)(error.cause).pipe(
          Effect.flatMap(({ code }) => (code === "ENOENT" ? Effect.succeed(undefined) : Effect.fail(error))),
        ),
      ),
      Effect.mapError(
        () => new RuntimeError({ operation: "oauth", message: `Cannot read T3 OAuth ${filename}.` }),
      ),
    )
    if (contents === undefined) return undefined
    return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(schema))(contents).pipe(
      // Schema errors can contain token values. Keep them out of errors and logs.
      Effect.mapError(
        () =>
          new RuntimeError({
            operation: "oauth",
            message: `Invalid T3 OAuth ${filename}. Sign in to T3 again.`,
          }),
      ),
    )
  })

  const save = Effect.fn("T3OAuth.save")(function* <A>(filename: string, schema: Schema.Codec<A>, value: A) {
    const contents = yield* Schema.encodeEffect(Schema.fromJsonString(schema))(value).pipe(
      Effect.mapError(
        () => new RuntimeError({ operation: "oauth", message: `Invalid T3 OAuth ${filename}.` }),
      ),
    )
    const temporary = join(directory, `.${filename}.${yield* Effect.sync(randomUUID)}.tmp`)
    yield* Effect.tryPromise(async () => {
      await mkdir(directory, { recursive: true, mode: 0o700 })
      await writeFile(temporary, contents, { mode: 0o600, flag: "wx" })
      await rename(temporary, join(directory, filename))
    }).pipe(
      Effect.ensuring(Effect.tryPromise(() => rm(temporary, { force: true })).pipe(Effect.ignore)),
      Effect.mapError(
        () => new RuntimeError({ operation: "oauth", message: `Cannot save T3 OAuth ${filename}.` }),
      ),
    )
  })

  const interactive = () => Effect.runPromise(Effect.fail(signInRequired(directory)))
  return {
    // The SDK only refreshes an expired token when the provider has a redirect URL. It is the one `atta login t3`
    // signs in with; nothing listens on it while atta runs, and a full sign-in still fails with signInRequired.
    redirectUrl: T3_REDIRECT_URL,
    clientMetadata: {
      client_name: "atta",
      redirect_uris: [T3_REDIRECT_URL],
      grant_types: ["authorization_code", "refresh_token"],
      response_types: ["code"],
      token_endpoint_auth_method: "none",
    },
    clientInformation: () => Effect.runPromise(read("client.json", ClientInformation)),
    saveClientInformation: (value) => Effect.runPromise(save("client.json", ClientInformation, value)),
    tokens: () => Effect.runPromise(read("tokens.json", Tokens)),
    saveTokens: (value) => Effect.runPromise(save("tokens.json", Tokens, value)),
    redirectToAuthorization: interactive,
    saveCodeVerifier: interactive,
    codeVerifier: interactive,
  }
}
