import { readFile } from "node:fs/promises"
import { homedir } from "node:os"
import { join, resolve } from "node:path"
import { pathToFileURL } from "node:url"
import { Config, Context, Effect, Layer, type Redacted, Schema } from "effect"
import { RepoGuides, Settings } from "../core/Settings.ts"
import { AttaConfig } from "../core/domain/config.ts"

export class ConfigLoadError extends Schema.TaggedError<ConfigLoadError>()("ConfigLoadError", {
  path: Schema.String,
  message: Schema.String,
}) {}

/** Secrets stay separate from the file config and are unwrapped only by the Slack adapter. */
export class SlackTokens extends Context.Service<
  SlackTokens,
  {
    readonly botToken: Redacted.Redacted<string>
    readonly appToken: Redacted.Redacted<string>
  }
>()("atta/app/SlackTokens") {}

const load = Effect.fn("Config.load")(function* (path: string) {
  const value: unknown = yield* Effect.tryPromise({
    try: async () => (await import(pathToFileURL(resolve(path)).href)).default,
    catch: (cause) =>
      new ConfigLoadError({
        path,
        message: `Could not load config ${path}: ${cause instanceof Error ? cause.message : String(cause)}`,
      }),
  })
  const config = yield* Schema.decodeUnknownEffect(AttaConfig)(value, { errors: "all" }).pipe(
    Effect.mapError(
      (error) => new ConfigLoadError({ path, message: `Invalid config ${path}: ${error.message}` }),
    ),
  )
  const home = yield* Effect.sync(homedir)
  const expand = (filePath: string) =>
    filePath === "~" ? home : filePath.startsWith("~/") ? join(home, filePath.slice(2)) : filePath

  return Settings.of({
    ...config,
    dataDir: expand(config.dataDir),
    repos: config.repos.map((repo) => ({ ...repo, path: expand(repo.path) })),
  })
})

/** Only the config file, for commands that do not talk to Slack (the logins). */
export const FileSettingsLayer = (path = "./atta.config.ts") => Layer.effect(Settings, load(path))

export const SettingsLayer = (path = "./atta.config.ts") =>
  Layer.effectContext(
    Effect.gen(function* () {
      const settings = yield* load(path)
      const tokens = yield* Config.all({
        botToken: Config.Redacted("SLACK_BOT_TOKEN"),
        appToken: Config.Redacted("SLACK_APP_TOKEN"),
      })
      return Context.make(Settings, settings).pipe(Context.add(SlackTokens, tokens))
    }),
  )

/** The most of a repository's AGENTS.md the router gets. A longer file is cut, with a note saying so. */
const MAX_GUIDE = 40_000

/** Reads each repository's AGENTS.md for the router. A repository without one simply has no guide. */
export const RepoGuidesLayer = Layer.effect(
  RepoGuides,
  Effect.gen(function* () {
    const settings = yield* Settings
    const guides = new Map<string, string>()
    for (const repo of settings.repos) {
      const text = yield* Effect.tryPromise(() => readFile(join(repo.path, "AGENTS.md"), "utf8")).pipe(
        Effect.catch(() =>
          Effect.logInfo(`${repo.name} has no AGENTS.md at ${repo.path}`).pipe(Effect.as(null)),
        ),
      )
      if (text === null) continue
      guides.set(
        repo.name,
        text.length > MAX_GUIDE ? `${text.slice(0, MAX_GUIDE)}\n\n(cut at ${MAX_GUIDE} characters)` : text,
      )
    }
    return RepoGuides.of(guides)
  }),
)
