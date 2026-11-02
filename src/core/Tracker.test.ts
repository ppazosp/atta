import { describe, expect, it } from "@effect/vitest"
import { RunId } from "./domain/ids.ts"
import type { Step } from "./domain/worker.ts"
import { taskTitle } from "./Tracker.ts"

const step = (title: string, kind: Step["kind"] = "command"): Step => ({
  id: "s",
  position: 1,
  runId: RunId.make("r"),
  kind,
  title,
  status: "complete",
  detail: null,
})

describe("plan task titles", () => {
  it("drops T3's shell wrapper and quotes", () => {
    expect(taskTitle(step(`$ /bin/zsh -lc 'pnpm test'`))).toBe("pnpm test")
    expect(taskTitle(step(`$ /bin/zsh -lc "git status --short"`))).toBe("git status --short")
  })

  it("keeps one short line", () => {
    const long = `$ /bin/zsh -lc "pwd; rg --files -g 'AGENTS.md' -g 'package.json' -g 'pyproject.toml' -g 'README*'\nmore"`
    const title = taskTitle(step(long))
    expect(title.length).toBeLessThanOrEqual(60)
    expect(title.endsWith("…")).toBe(true)
    expect(title).not.toContain("\n")
  })

  it("keeps a real title as it is, and names an empty one by its kind", () => {
    expect(taskTitle(step("Workspace ready"))).toBe("Workspace ready")
    expect(taskTitle(step("", "file_change"))).toBe("file change")
  })
})
