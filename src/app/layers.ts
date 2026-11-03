import { join } from "node:path"
import { WebClient } from "@slack/web-api"
import { Effect, Layer, Redacted, Schema } from "effect"
import { ClockLayer } from "../adapters/clock/index.ts"
import { MemoryNoneLayer } from "../adapters/memory-none/index.ts"
import { PiDurableRouterEngineLayer } from "../adapters/pi-durable/index.ts"
import { SlackChannelLayer } from "../adapters/slack/index.ts"
import { SqliteStoreLayer } from "../adapters/sqlite/index.ts"
import { T3RuntimeLayer } from "../adapters/t3/index.ts"
import { CoreBaseLayer, CoreLayer } from "../core/run.ts"
import { Settings } from "../core/Settings.ts"
import { McpServerLayer } from "../mcp/index.ts"
import { ChannelError } from "../ports/Channel.ts"
import { RepoGuidesLayer, SettingsLayer, SlackTokens } from "./config.ts"

/** Where atta keeps its files, all under `dataDir`. */
export const paths = (dataDir: string) => ({
  database: join(dataDir, "atta.sqlite"),
  router: join(dataDir, "router.sqlite"),
  routerCredentials: join(dataDir, "pi"),
  t3Tokens: join(dataDir, "t3-mcp"),
})

const settings = Effect.gen(function* () {
  return yield* Settings
})

const AuthTest = Schema.Struct({ ok: Schema.Literal(true), user_id: Schema.NonEmptyString })

/** Slack, with the bot's own user ID looked up from its token. */
const ChannelLive = Layer.unwrap(
  Effect.gen(function* () {
    const tokens = yield* SlackTokens
    const botUserId = yield* Effect.tryPromise({
      try: () => new WebClient(Redacted.value(tokens.botToken)).auth.test(),
      catch: (cause) => new ChannelError({ operation: "auth.test", message: String(cause) }),
    }).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(AuthTest)),
      Effect.map((result) => result.user_id),
      Effect.mapError((e) => new ChannelError({ operation: "auth.test", message: e.message })),
    )
    return SlackChannelLayer({
      botToken: Redacted.value(tokens.botToken),
      appToken: Redacted.value(tokens.appToken),
      botUserId,
    })
  }),
)

const StoreLive = Layer.unwrap(Effect.map(settings, (s) => SqliteStoreLayer(paths(s.dataDir).database)))

const RuntimeLive = Layer.unwrap(
  settings.pipe(
    Effect.map((s) =>
      T3RuntimeLayer({
        url: s.t3.url,
        tokenDirectory: paths(s.dataDir).t3Tokens,
        repos: s.repos.map((r) => ({
          name: r.name,
          t3ProjectId: r.t3ProjectId,
          baseBranch: r.baseBranch ?? "main",
        })),
        instanceId: s.workers.t3InstanceId,
        model: s.workers.model,
        effort: s.workers.effort,
      }),
    ),
  ),
)

const EngineLive = Layer.unwrap(
  settings.pipe(
    Effect.map((s) =>
      PiDurableRouterEngineLayer({
        storagePath: paths(s.dataDir).router,
        credentialsDir: paths(s.dataDir).routerCredentials,
        modelId: s.router.model,
        thinkingLevel: s.router.effort,
      }),
    ),
  ),
)

/**
 * The whole app: ports, then core's base (which provides the router's tools), then the router engine, then the
 * rest of core, then atta's MCP server for workers.
 */
export const AppLayer = (configPath: string) => {
  const ports = Layer.mergeAll(
    StoreLive,
    ChannelLive,
    RuntimeLive,
    MemoryNoneLayer,
    ClockLayer,
    RepoGuidesLayer,
  ).pipe(Layer.provideMerge(SettingsLayer(configPath)))
  const base = CoreBaseLayer.pipe(Layer.provideMerge(ports))
  const engine = EngineLive.pipe(Layer.provideMerge(base))
  const core = CoreLayer.pipe(Layer.provideMerge(engine))
  return McpServerLayer.pipe(Layer.provideMerge(core))
}
