import { describe, expect, it } from "@effect/vitest"
import { Result } from "effect"
import { allArrows, transition } from "./lifecycle.ts"
import { WorkerState } from "./worker.ts"

// Every arrow drawn in docs/worker-lifecycle.png. If the statechart changes, change both.
const drawn = [
  ["launching", "launched", "running"],
  ["launching", "launch_failed", "failed"],
  ["running", "step", "running"],
  ["running", "steered", "running"],
  ["running", "reported", "running"],
  ["running", "run_completed", "idle"],
  ["running", "run_interrupted", "interrupted"],
  ["running", "run_failed", "failed"],
  ["running", "stop_requested", "stopping"],
  ["idle", "follow_up", "running"],
  ["idle", "idle_expired", "retired"],
  ["interrupted", "continue", "running"],
  ["interrupted", "drop", "retired"],
  ["failed", "retry", "running"],
  ["failed", "give_up", "retired"],
  ["stopping", "stopped", "retired"],
] as const

describe("worker lifecycle", () => {
  it("has exactly the arrows of the statechart", () => {
    expect([...allArrows()].toSorted()).toEqual([...drawn].map((a) => [...a]).toSorted())
  })

  it.each(drawn)("%s --%s--> %s", (from, input, to) => {
    expect(transition(from, input)).toEqual(Result.succeed(to))
  })

  it("rejects anything not drawn", () => {
    const result = transition("idle", "stop_requested")
    expect(Result.isFailure(result)).toBe(true)
  })

  it("never leaves retired", () => {
    for (const [, input] of drawn) expect(Result.isFailure(transition("retired", input))).toBe(true)
  })

  it("reaches every state", () => {
    const reached = new Set<string>(["launching", ...drawn.map(([, , to]) => to)])
    expect([...reached].toSorted()).toEqual([...WorkerState.literals].toSorted())
  })
})
