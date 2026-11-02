import { mkdtemp, rm, writeFile } from "node:fs/promises"
import { homedir, tmpdir } from "node:os"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
import { describe, expect, it } from "@effect/vitest"
import { ConfigProvider, Effect, Redacted } from "effect"
import { vi } from "vitest"
import example from "../../atta.config.example.ts"
import { Settings } from "../core/Settings.ts"
import { ConfigLoadError, SettingsLayer, SlackTokens } from "./config.ts"

const examplePath = fileURLToPath(new URL("../../atta.config.example.ts", import.meta.url))
const environment = {
  SLACK_BOT_TOKEN: "fixture-bot-token",
  SLACK_APP_TOKEN: "fixture-app-token",
}
const provider = ConfigProvider.fromEnv({ env: environment })

const temporaryConfig = Effect.fn("ConfigTest.temporaryConfig")(function* (source: string) {
  const directory = yield* Effect.acquireRelease(
    Effect.tryPromise(() => mkdtemp(join(tmpdir(), "atta-config-"))),
    (path) => Effect.promise(() => rm(path, { recursive: true, force: true })),
  )
  const path = join(directory, "atta.config.ts")
  yield* Effect.tryPromise(() => writeFile(path, source))
  return path
})

// The annotation also verifies that the loader imports TypeScript config files.
const configSource = (config: unknown) => `const config: unknown = ${JSON.stringify(config)}
export default config
`

describe("SettingsLayer", () => {
  it.effect("dynamically loads the example and supplies redacted environment tokens", () =>
    Effect.gen(function* () {
      const config = yield* Settings
      const tokens = yield* SlackTokens
      expect(config).toEqual({ ...example, dataDir: join(homedir(), ".atta") })
      expect(Redacted.isRedacted(tokens.botToken)).toBe(true)
      expect(Redacted.isRedacted(tokens.appToken)).toBe(true)
      expect(Redacted.value(tokens.botToken)).toBe(environment.SLACK_BOT_TOKEN)
      expect(Redacted.value(tokens.appToken)).toBe(environment.SLACK_APP_TOKEN)
      expect(JSON.stringify(tokens)).not.toContain(environment.SLACK_BOT_TOKEN)
      expect(JSON.stringify(tokens)).not.toContain(environment.SLACK_APP_TOKEN)
      expect(config).not.toHaveProperty("botToken")
    }).pipe(
      Effect.provide(SettingsLayer(examplePath)),
      Effect.provideService(ConfigProvider.ConfigProvider, provider),
    ),
  )

  it.effect.each(["~", "~/atta-data"])("expands home paths in dataDir and every repository: %s", (dataDir) =>
    Effect.gen(function* () {
      const paths = ["~", "~/code/repo", "./relative", "/absolute/repo", "~someone/repo"]
      const path = yield* temporaryConfig(
        configSource({
          ...example,
          dataDir,
          repos: paths.map((repoPath, index) => ({
            ...example.repos[0],
            name: `repo-${index}`,
            path: repoPath,
          })),
        }),
      )
      const config = yield* Settings.pipe(Effect.provide(SettingsLayer(path)))
      expect(config.dataDir).toBe(dataDir === "~" ? homedir() : join(homedir(), "atta-data"))
      expect(config.repos.map((repo) => repo.path)).toEqual([
        homedir(),
        join(homedir(), "code/repo"),
        "./relative",
        "/absolute/repo",
        "~someone/repo",
      ])
    }).pipe(Effect.provideService(ConfigProvider.ConfigProvider, provider), Effect.scoped),
  )

  it.effect("defaults to atta.config.ts in the current directory", () =>
    Effect.gen(function* () {
      const path = yield* temporaryConfig(configSource({ ...example, dataDir: "./fixture-data" }))
      yield* Effect.acquireRelease(
        Effect.sync(() => vi.spyOn(process, "cwd").mockReturnValue(dirname(path))),
        (spy) => Effect.sync(() => spy.mockRestore()),
      )
      const config = yield* Settings.pipe(Effect.provide(SettingsLayer()))
      expect(config.dataDir).toBe("./fixture-data")
    }).pipe(Effect.provideService(ConfigProvider.ConfigProvider, provider), Effect.scoped),
  )

  it.effect.each([
    { config: { ...example, mcp: { port: 80 } }, fields: ["mcp", "port"] },
    { config: { ...example, repos: [{ ...example.repos[0], path: 42 }] }, fields: ["repos", "path"] },
    {
      config: { ...example, router: { ...example.router, effort: "extreme" } },
      fields: ["router", "effort"],
    },
  ])("names the bad config field %#", ({ config, fields }) =>
    Effect.gen(function* () {
      const path = yield* temporaryConfig(configSource(config))
      const error = yield* Settings.pipe(Effect.provide(SettingsLayer(path)), Effect.flip)
      expect(error).toBeInstanceOf(ConfigLoadError)
      expect(error.message).toContain(path)
      for (const field of fields) expect(error.message).toContain(field)
    }).pipe(Effect.provideService(ConfigProvider.ConfigProvider, provider), Effect.scoped),
  )

  it.effect.each([
    "export const config = {}",
    "export default {",
    'throw new Error("fixture import failure")',
  ])("reports a config without a valid default export %#", (source) =>
    Effect.gen(function* () {
      const path = yield* temporaryConfig(source)
      const error = yield* Settings.pipe(Effect.provide(SettingsLayer(path)), Effect.flip)
      expect(error).toBeInstanceOf(ConfigLoadError)
      expect(error.message).toContain(path)
    }).pipe(Effect.provideService(ConfigProvider.ConfigProvider, provider), Effect.scoped),
  )

  it.effect("reports a missing config file", () =>
    Effect.gen(function* () {
      const path = `${yield* temporaryConfig(configSource(example))}.missing.ts`
      const error = yield* Settings.pipe(Effect.provide(SettingsLayer(path)), Effect.flip)
      expect(error).toBeInstanceOf(ConfigLoadError)
      expect(error.message).toContain(path)
    }).pipe(Effect.provideService(ConfigProvider.ConfigProvider, provider), Effect.scoped),
  )

  it.effect.each(["SLACK_BOT_TOKEN", "SLACK_APP_TOKEN"])("names the missing token %s", (name) =>
    Effect.gen(function* () {
      const env = Object.fromEntries(Object.entries(environment).filter(([key]) => key !== name))
      const error = yield* Settings.pipe(
        Effect.provide(SettingsLayer(examplePath)),
        Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnv({ env })),
        Effect.flip,
      )
      expect(error._tag).toBe("ConfigError")
      expect(error.message).toContain(name)
      expect(error.message).not.toContain(environment.SLACK_BOT_TOKEN)
      expect(error.message).not.toContain(environment.SLACK_APP_TOKEN)
    }),
  )
})
