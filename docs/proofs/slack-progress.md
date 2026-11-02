# Findings: Slack streaming for Physalia progress

> Written during the proofs, when the project was called Physalia and lived outside this repository. Paths like `~/projects/physalia/poc` refer to that proof folder, which is not published.

Status: everything run in a DM with the bot, in a personal test workspace (Acme Inc.), on 2 November 2026. Channel threads and mobile have **not** been tested yet.

Every claim is tagged: **measured** (seen in this proof, from `logs/calls.jsonl` or by reading the message back from Slack), **inferred** (from Slack's docs, the SDK types or other measurements) or **guess**.

## Summary

- **Pablo's preferred look is `demo agent`:** while the worker runs, the message shows only tasks in plan view, one task per command, file edit or step. The written answer appears once at the end (measured: Pablo's reaction, "SO GOOD").
- **Everything worked in the DM.** Every call returned `ok` except one, which was expected: `static_prompts_configured`, see below (measured).
- **A stream lasts 10 minutes, then Slack closes it** (measured: 10.1 minutes). After that, `appendStream` and `stopStream` both fail with `message_not_in_streaming_state`, but `chat.update` can still edit the message (measured).
- **Rate limits were not a problem for one stream:** 300 appends back to back, no `ratelimited` (measured).
- **Recommendation for Physalia (inferred):** draw the plan view with `chat.postMessage` + `chat.update` on a `plan` block, not with a stream. Both are stored as the same `plan` block, and `chat.update` has no 10-minute limit. `demo agent-update` shows this, so Pablo can check that it looks the same. Keep streams for short runs (under about 9 minutes) or the final answer only.

## Live results (DM / Messages tab)

Setup:

- The manifest with `agent_view` was accepted, and the bot connected over Socket Mode (measured).
- Opening the Messages tab fired `app_home_opened` with `tab: "messages"` (measured).
- `assistant.threads.setSuggestedPrompts` fails with `static_prompts_configured` when the manifest already sets `suggested_prompts` (measured). Set prompts either in the manifest or in code, not both.

`demo all`: 61 calls, all `ok`, no warnings, 141 to 533 ms each, 70 seconds for the whole run (measured). What Slack stored, read back with `conversations.replies` (measured):

- **Text stream.** The markdown is stored as normal blocks: the `###` heading became a `header` block, the rest `rich_text`, and the feedback buttons a `context_actions` block at the end.
- **Markdown flavour.** `markdown_text` is standard Markdown, not Slack's mrkdwn: `*text*` became italic, not bold (measured). Use `**bold**`, `- lists` and `[text](url)` links.
- **Timeline mode.** Each `task_update` id became one `task_card` block, interleaved with the text in the order the tasks first appeared. Later updates to the same id changed that card in place.
- **Unfinished tasks become errors.** A task still `pending` when the stream stopped was stored as `error` (measured). A router must close every task before stopping, or the run will look failed.
- **Plan mode.** The whole run became one `plan` block holding every task with its final status, titled with the last `plan_update`.
- **`chat.update` on a `plan` block of `task_card`s** worked: 4 edits, and the final message holds the plan block plus feedback buttons. A standalone `task_card` outside a plan block is also accepted.
- **`blocks` chunks.** Sending a `task_card` twice with the same `task_id` in `blocks` chunks **adds a second card**; the first stays `in_progress`. Use `task_update` chunks for progress, not block chunks.
- **Feedback buttons.** Pablo clicked "Good"; the `feedback` action arrived with `value=good` and the ephemeral reply was sent.
- **Reactions.** Adding 👀, removing it and adding ✅ all worked.
- **`setStatus`** returned `ok`. Whether the loading messages rotated is for Pablo to confirm by eye.

`demo agent` (plan view, tasks only, answer at the end): 17 calls, all `ok`. Stored as one `plan` block with 8 tasks (7 complete, 1 error), then the answer as `rich_text` and the feedback buttons (measured). Starting a stream with only a `plan_update` and a `task_update`, with no text at all, works (measured).

| Feature                                      | DM / Messages tab              | Channel thread | Mobile          |
| -------------------------------------------- | ------------------------------ | -------------- | --------------- |
| `setStatus` + loading messages               | ok (measured)                  | not yet run    | not yet checked |
| Markdown stream (`ChatStreamer`)             | ok (measured)                  | not yet run    | not yet checked |
| `task_update`, timeline                      | ok (measured)                  | not yet run    | not yet checked |
| `task_update` + `plan_update`, plan          | ok (measured)                  | not yet run    | not yet checked |
| Tasks only, answer at the end (`demo agent`) | ok, preferred (measured)       | not yet run    | not yet checked |
| `plan` / `task_card` via `chat.update`       | ok (measured)                  | not yet run    | not yet checked |
| `blocks` chunk with `task_card`              | ok, but adds a card (measured) | not yet run    | not yet checked |
| Feedback buttons                             | ok (measured)                  | not yet run    | not yet checked |
| Reactions                                    | ok (measured)                  | not yet run    | not yet checked |

## Limits measured

- **Stream lifetime.** One stream, one append a minute: appends 1 to 10 succeeded; the append at 10.1 minutes failed with `message_not_in_streaming_state` (measured). The following `stopStream` failed with the same code, so the final answer and feedback buttons can't be added to a timed-out stream. `chat.update` on the same message afterwards succeeded and replaced its content (measured).
- **Append rate.** 120 back-to-back `appendStream` calls to one stream: all ok in 21.2 seconds. 300 back to back: all ok in 53.1 seconds, about 340 a minute, no `ratelimited` (measured, one stream at a time, about 177 ms each). That is well above the documented Tier 4 rate of about 100 a minute. Inferred: Slack allows bursts, or applies the limit per stream. Several streams at once were not tested.
- **Bot killed mid-stream.** Restarting the bot during `demo agent-timeline` left a stream open after 4 appends. Eleven minutes later, Slack had closed it by itself: the two finished tasks stayed `complete`, the task that was running became `error`, and there was no answer and no feedback buttons (measured). So if Physalia crashes, the user sees a failed task, not a spinner that never stops. That holds only for the stream design; a `chat.update` message would stay `in_progress` forever unless something cleans it up (inferred).

## From Slack's docs, not tested here (inferred)

- Per stream: 12,000 characters of markdown, 50 unique tasks, 50 sources per task, 50 blocks (plus 50 more on `stopStream`). A worker that runs more than 50 commands needs grouping, or several messages.
- `chat.startStream` is Tier 2 (about 20 a minute). `chat.update` is Tier 3 (about 50 a minute per channel, guess from the usual tier), so the `chat.update` design should throttle edits to about one a second.
- In channels, streams need `recipient_user_id` and `recipient_team_id` (the bot always sends both), and must be in a thread.
- Text and chunks cannot be mixed in one stream (`streaming_mode_mismatch`). The SDK's `ChatStreamer` always uses chunks.
- In `agent_view`, a `setStatus` status expires after 2 minutes without a reply.
- `assistant_view` is being replaced by `agent_view`; new apps can't use the old one and the switch is one-way.

## API shapes in the installed packages (measured, read from the `.d.ts` files)

- Chunks: `markdown_text {text}`, `plan_update {title}`, `task_update {id, title, status, details?, output?, sources?}` and `blocks {blocks}`. Task status: `pending | in_progress | complete | error`.
- `chat.startStream` takes `task_display_mode` (`timeline` or `plan`), `recipient_user_id` and `recipient_team_id`. `chat.stopStream` takes `chunks`, `blocks` and `session_status`.
- `task_card` has `task_id`, `title`, `status`, `sources`, and `details` / `output` as one `rich_text` block each. `plan` has `title` and `tasks`.
- `feedback_buttons` is an element inside a `context_actions` block, not a block of its own.

## Not tested (the proof was closed before these ran)

- Run `demo agent` and `demo agent-update` in a channel thread (`@physaliapoc demo agent`), to compare channels with the DM.
- Check `demo agent` and `demo agent-update` on mobile.
- Confirm by eye that `demo agent-update` looks the same as `demo agent`.
