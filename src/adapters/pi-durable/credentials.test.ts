import { chmod, readFile, readdir, stat, writeFile } from "node:fs/promises"
import { join } from "node:path"
import { describe, expect, it } from "@effect/vitest"
import { Deferred, Effect, Fiber } from "effect"
import { openCredentials } from "./credentials.ts"
import { temporaryDirectory } from "./test-support.ts"

const credential = {
  type: "oauth" as const,
  access: "fixture-access",
  refresh: "fixture-refresh",
  expires: 123456,
  clientId: "fixture-client",
  nested: { scope: ["fixture"] },
}

describe("Pi credentials", () => {
  it.effect("persists OAuth fields and a stable device ID, with private files and directory", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const first = yield* openCredentials(directory)
      expect(yield* Effect.tryPromise(() => first.store.read("openai"))).toBeUndefined()
      expect(yield* Effect.tryPromise(() => first.store.read("toString"))).toBeUndefined()
      expect(yield* Effect.tryPromise(() => first.store.list())).toEqual([])
      yield* Effect.tryPromise(() => first.store.modify("openai", async () => credential))
      yield* Effect.tryPromise(() =>
        first.store.modify("other", async () => ({ type: "api_key", key: "fixture-key" })),
      )
      expect(yield* Effect.tryPromise(() => first.store.modify("openai", async () => undefined))).toEqual(
        credential,
      )
      const reopened = yield* openCredentials(directory)
      expect(reopened.deviceId).toBe(first.deviceId)
      expect(yield* Effect.tryPromise(() => reopened.store.read("openai"))).toEqual(credential)
      expect(yield* Effect.tryPromise(() => reopened.store.list())).toEqual([
        { providerId: "openai", type: "oauth" },
        { providerId: "other", type: "api_key" },
      ])
      yield* Effect.tryPromise(() => reopened.store.delete("openai"))
      expect(yield* Effect.tryPromise(() => first.store.read("openai"))).toBeUndefined()
      expect(yield* Effect.tryPromise(() => first.store.read("other"))).toEqual({
        type: "api_key",
        key: "fixture-key",
      })
      expect((yield* Effect.tryPromise(() => stat(directory))).mode & 0o777).toBe(0o700)
      for (const name of ["pi-auth.json", "pi-device-id"]) {
        expect((yield* Effect.tryPromise(() => stat(join(directory, name)))).mode & 0o777).toBe(0o600)
      }
      expect((yield* Effect.tryPromise(() => readdir(directory))).toSorted()).toEqual([
        "pi-auth.json",
        "pi-device-id",
      ])
    }).pipe(Effect.scoped),
  )

  it.effect("tightens permissions on existing files", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const first = yield* openCredentials(directory)
      yield* Effect.tryPromise(() => first.store.modify("openai", async () => credential))
      yield* Effect.tryPromise(async () => {
        await chmod(directory, 0o755)
        await chmod(join(directory, "pi-auth.json"), 0o644)
        await chmod(join(directory, "pi-device-id"), 0o644)
      })
      const reopened = yield* openCredentials(directory)
      expect(reopened.deviceId).toBe(first.deviceId)
      expect((yield* Effect.tryPromise(() => stat(directory))).mode & 0o777).toBe(0o700)
      for (const name of ["pi-auth.json", "pi-device-id"]) {
        expect((yield* Effect.tryPromise(() => stat(join(directory, name)))).mode & 0o777).toBe(0o600)
      }
    }).pipe(Effect.scoped),
  )

  it.effect("serializes refresh callbacks across store instances and preserves other providers", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const first = yield* openCredentials(directory)
      const second = yield* openCredentials(directory)
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const refreshing = yield* Effect.tryPromise(() =>
        first.store.modify("openai", async () => {
          await Effect.runPromise(Deferred.succeed(started, undefined))
          await Effect.runPromise(Deferred.await(release))
          return credential
        }),
      ).pipe(Effect.forkChild)
      yield* Deferred.await(started)
      const other = yield* Effect.tryPromise(() =>
        second.store.modify("other", async () => ({ type: "api_key", key: "fixture-key" })),
      ).pipe(Effect.forkChild)
      const rotated = yield* Effect.tryPromise(() =>
        second.store.modify("openai", async (current) => {
          expect(current).toEqual(credential)
          return { ...credential, refresh: "rotated-fixture" }
        }),
      ).pipe(Effect.forkChild)
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(refreshing)
      yield* Fiber.join(other)
      yield* Fiber.join(rotated)
      expect(yield* Effect.tryPromise(() => first.store.read("openai"))).toEqual({
        ...credential,
        refresh: "rotated-fixture",
      })
      expect(yield* Effect.tryPromise(() => first.store.read("other"))).toEqual({
        type: "api_key",
        key: "fixture-key",
      })
    }).pipe(Effect.scoped),
  )

  it.effect("releases the lock on failure, keeps prior credentials, and honors cancellation", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      const { store } = yield* openCredentials(directory)
      yield* Effect.tryPromise(() => store.modify("openai", async () => credential))
      const failure = yield* Effect.tryPromise(() =>
        store.modify("openai", () => Promise.reject(new Error("fixture-secret"))),
      ).pipe(Effect.flip)
      expect(String(failure.cause)).not.toContain("fixture-secret")
      expect(yield* Effect.tryPromise(() => store.read("openai"))).toEqual(credential)
      const signal = AbortSignal.abort()
      const canceled = yield* Effect.tryPromise(() =>
        store.modify("openai", async () => undefined, { signal }),
      ).pipe(Effect.flip)
      expect(String(canceled.cause)).toContain("Cannot read or save Pi credentials")
      yield* Effect.tryPromise(() => store.delete("openai"))
      expect(yield* Effect.tryPromise(() => store.read("openai"))).toBeUndefined()
    }).pipe(Effect.scoped),
  )

  it.effect.each(["not json", '{"openai":{"type":"oauth","access":"fixture-secret","expires":false}}'])(
    "rejects corrupt credentials without exposing values %#",
    (contents) =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory
        const { store } = yield* openCredentials(directory)
        yield* Effect.tryPromise(() => writeFile(join(directory, "pi-auth.json"), contents))
        const failure = yield* Effect.tryPromise(() => store.read("openai")).pipe(Effect.flip)
        expect(String(failure.cause)).toContain("Cannot read or save Pi credentials")
        expect(String(failure.cause)).not.toContain("fixture-secret")
        expect(yield* Effect.tryPromise(() => readFile(join(directory, "pi-auth.json"), "utf8"))).toBe(
          contents,
        )
      }).pipe(Effect.scoped),
  )

  it.effect("fails on a corrupt device ID rather than changing the installation identity", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory
      yield* Effect.tryPromise(() => writeFile(join(directory, "pi-device-id"), "invalid-device"))
      expect(yield* openCredentials(directory).pipe(Effect.flip)).toMatchObject({
        _tag: "EngineError",
        operation: "credentials",
      })
      expect(yield* Effect.tryPromise(() => readFile(join(directory, "pi-device-id"), "utf8"))).toBe(
        "invalid-device",
      )
    }).pipe(Effect.scoped),
  )
})
