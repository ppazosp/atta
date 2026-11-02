import { describe, expect, it } from "@effect/vitest"
import { finalTasks } from "./Workers.ts"

const tasks = [
  { id: "1", title: "Read", status: "complete", detail: null },
  { id: "2", title: "Fix", status: "in_progress", detail: null },
  { id: "3", title: "Test", status: "pending", detail: null },
] as const

describe("the plan's last state", () => {
  it("ticks forgotten steps when the run completed", () => {
    expect(finalTasks(tasks, "completed").map((t) => t.status)).toEqual(["complete", "complete", "complete"])
  })

  it.each(["failed", "stopped", "interrupted"] as const)(
    "marks unfinished steps as errors when the run %s",
    (outcome) => {
      expect(finalTasks(tasks, outcome).map((t) => t.status)).toEqual(["complete", "error", "error"])
    },
  )
})
