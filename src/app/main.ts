import { NodeRuntime } from "@effect/platform-node"
import { Console, Effect, Layer } from "effect"
import { login as routerLogin } from "../adapters/pi-durable/index.ts"
import { runCore } from "../core/run.ts"
import { Settings } from "../core/Settings.ts"
import { SettingsLayer } from "./config.ts"
import { AppLayer, paths } from "./layers.ts"
import { t3Login } from "./t3Login.ts"
import { version } from "./version.ts"

const configPath = process.env["ATTA_CONFIG"] ?? "./atta.config.ts"
const [command = "start", target] = process.argv.slice(2)

const usage = `atta ${version}

  atta start          run atta (Slack, router, workers)
  atta login router   sign the router in to ChatGPT
  atta login t3       sign atta in to T3 Code

Config: ATTA_CONFIG or ./atta.config.ts. Slack tokens: SLACK_BOT_TOKEN, SLACK_APP_TOKEN.`

const settings = Effect.gen(function* () {
  return yield* Settings
})

const program: Effect.Effect<void, unknown> =
  command === "start"
    ? Layer.launch(Layer.effectDiscard(runCore).pipe(Layer.provide(AppLayer(configPath))))
    : command === "login" && target === "router"
      ? settings.pipe(
          Effect.flatMap((s) => routerLogin(paths(s.dataDir).routerCredentials)),
          Effect.provide(SettingsLayer(configPath)),
        )
      : command === "login" && target === "t3"
        ? settings.pipe(
            Effect.flatMap((s) => t3Login(s.t3.url, paths(s.dataDir).t3Tokens)),
            Effect.provide(SettingsLayer(configPath)),
          )
        : Console.log(usage)

NodeRuntime.runMain(program)
