import { NodeRuntime } from "@effect/platform-node"
import { Effect } from "effect"
import { version } from "./version.ts"

// Placeholder entry point until milestone 1 wires the layers.
const main = Effect.log(`atta ${version}: nothing is wired yet`)

NodeRuntime.runMain(main)
