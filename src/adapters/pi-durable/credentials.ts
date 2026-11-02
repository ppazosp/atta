import { randomUUID } from "node:crypto"
import { chmod, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises"
import { join, resolve } from "node:path"
import { processLock } from "@earendil-works/chord/node"
import type { AuthOperationOptions, CredentialStore } from "@earendil-works/pi-ai"
import { Effect, Schedule, Schema } from "effect"
import { EngineError } from "../../ports/RouterEngine.ts"

const CredentialSchema = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("api_key"),
    key: Schema.optionalKey(Schema.String),
    env: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  }),
  Schema.StructWithRest(
    Schema.Struct({
      type: Schema.Literal("oauth"),
      refresh: Schema.String,
      access: Schema.String,
      expires: Schema.Number,
    }),
    [Schema.Record(Schema.String, Schema.Json)],
  ),
])
const Credentials = Schema.Record(Schema.String, CredentialSchema)
const CredentialsJson = Schema.fromJsonString(Credentials)
const FileError = Schema.Struct({ code: Schema.String })
const Locked = Schema.Struct({ code: Schema.Literal("unavailable"), reason: Schema.Literal("locked") })
const DeviceId = Schema.String.check(Schema.isUUID())

const failure = () =>
  new EngineError({ operation: "credentials", message: "Cannot read or save Pi credentials." })

const checkSignal = (options?: AuthOperationOptions) => Effect.try(() => options?.signal?.throwIfAborted())

const currentCredential = (credentials: typeof Credentials.Type, providerId: string) =>
  Object.hasOwn(credentials, providerId) ? credentials[providerId] : undefined

/** Atomic replacement also keeps an existing credential file private. */
const save = Effect.fn("PiCredentials.save")(function* (path: string, contents: string) {
  const temporary = `${path}.${yield* Effect.sync(randomUUID)}.tmp`
  yield* Effect.tryPromise(async () => {
    await writeFile(temporary, contents, { mode: 0o600, flag: "wx" })
    await rename(temporary, path)
  }).pipe(Effect.ensuring(Effect.tryPromise(() => rm(temporary, { force: true })).pipe(Effect.orDie)))
})

const read = Effect.fn("PiCredentials.readFile")(function* (path: string) {
  return yield* Effect.tryPromise(() => readFile(path, "utf8")).pipe(
    Effect.catch((error) =>
      Schema.is(FileError)(error.cause) && error.cause.code === "ENOENT"
        ? Effect.succeed(undefined)
        : Effect.fail(error),
    ),
  )
})

export const openCredentials = Effect.fn("PiCredentials.open")(function* (directory: string) {
  const path = join(resolve(directory), "pi-auth.json")
  const devicePath = join(resolve(directory), "pi-device-id")
  yield* Effect.tryPromise(async () => {
    await mkdir(directory, { recursive: true, mode: 0o700 })
    await chmod(directory, 0o700)
  }).pipe(Effect.mapError(failure))

  const locked = Effect.fn("PiCredentials.locked")(
    function* <A, E>(effect: Effect.Effect<A, E>) {
      yield* Effect.acquireRelease(
        Effect.tryPromise(() => processLock(path)).pipe(
          Effect.retry({
            while: (error) => Schema.is(Locked)(error.cause),
            schedule: Schedule.spaced("25 millis"),
            times: 1200,
          }),
        ),
        (release) => Effect.tryPromise(release).pipe(Effect.mapError(failure), Effect.orDie),
      )
      return yield* effect
    },
    Effect.scoped,
    Effect.mapError(failure),
  )

  const load = Effect.gen(function* () {
    const contents = yield* read(path)
    if (contents === undefined) return {}
    return yield* Schema.decodeUnknownEffect(CredentialsJson)(contents)
  })
  const persist = Effect.fn("PiCredentials.persist")(function* (credentials: typeof Credentials.Type) {
    yield* save(path, yield* Schema.encodeEffect(CredentialsJson)(credentials))
  })

  // The lock covers the callback too: refresh tokens can rotate while it runs.
  const store: CredentialStore = {
    read: (providerId, options) =>
      Effect.runPromise(
        checkSignal(options).pipe(
          Effect.andThen(load),
          Effect.map((credentials) => currentCredential(credentials, providerId)),
          Effect.mapError(failure),
        ),
      ),
    list: (options) =>
      Effect.runPromise(
        checkSignal(options).pipe(
          Effect.andThen(load),
          Effect.map((credentials) =>
            Object.entries(credentials).map(([providerId, credential]) => ({
              providerId,
              type: credential.type,
            })),
          ),
          Effect.mapError(failure),
        ),
      ),
    modify: (providerId, fn, options) =>
      Effect.runPromise(
        locked(
          Effect.gen(function* () {
            yield* checkSignal(options)
            const credentials = yield* load
            const current = currentCredential(credentials, providerId)
            const next = yield* Effect.tryPromise(() => fn(current))
            yield* checkSignal(options)
            if (next === undefined) return current
            const decoded = yield* Schema.decodeUnknownEffect(CredentialSchema)(next)
            yield* persist({ ...credentials, [providerId]: decoded })
            return decoded
          }),
        ),
      ),
    delete: (providerId, options) =>
      Effect.runPromise(
        locked(
          Effect.gen(function* () {
            yield* checkSignal(options)
            const credentials = { ...(yield* load) }
            delete credentials[providerId]
            yield* persist(credentials)
          }),
        ),
      ),
  }

  const deviceId = yield* locked(
    Effect.gen(function* () {
      const credentials = yield* read(path)
      if (credentials !== undefined) yield* Effect.tryPromise(() => chmod(path, 0o600))
      const existing = yield* read(devicePath)
      if (existing !== undefined) {
        yield* Effect.tryPromise(() => chmod(devicePath, 0o600))
        return yield* Schema.decodeUnknownEffect(DeviceId)(existing.trim())
      }
      const id = yield* Effect.sync(randomUUID)
      yield* save(devicePath, id)
      return id
    }),
  )
  return { store, deviceId }
})
