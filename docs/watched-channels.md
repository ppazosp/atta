# Watched channels

atta can read channels you add it to and propose work it finds there. It never posts, reacts or starts work in a watched channel until you approve, with one click, in a direct message.

## How it works

1. You add atta to a channel and put the channel ID in `watchedChannels` in `atta.config.ts`.
2. Every message in that channel, from anyone, goes to the observer: a second router conversation, on the same model, kept apart from the router. It sees the text quoted as untrusted, with a note that it is not a request.
3. Most observations need nothing, so the observer ends its turn without calling a tool.
4. When a message asks for clear, worthwhile work in a known repository, the observer calls `propose_work` with a title, a reason and a brief.
5. You get a direct message with the title, the reason, the whole brief, the repository, a link to the thread, and Start and Skip buttons.
6. On Start, atta opens that channel thread and starts the worker there. Its plan and result appear in the thread, as they do for work you ask for directly.
7. On Skip, nothing runs. Either way, the buttons are replaced by the outcome ("Started: …", "Skipped: …" or "Could not start: …").

## What stays true

These rules hold in the code and are covered by tests in `test/core.test.ts` and `src/core/domain/routing.test.ts`:

- channel text never reaches the router: the observer has only `propose_work`, `list_workers` and `recall`, so text written to trick it can at most ask you for approval
- you approve exactly what runs: the direct message shows the whole brief, and a brief too long to show (over 2,000 characters) is refused
- atta posts and reacts only in threads you started with it, or that you approved with Start
- a message from anyone else is only ever observed: it never reaches a worker, even in a thread where a worker runs
- only your click counts; anyone else's click is dropped
- a replayed `propose_work` call sends one direct message, and a replayed decision starts one worker
- in a watched channel, your own messages are observed too, unless you mention atta, which makes them a normal request

## Setting it up

Add the channel IDs to your config. The Slack app already has the scopes and events it needs (`channels:history`, `groups:history`, `message.channels`, `message.groups`, `im:write` and interactivity), so you do not need to reinstall it.

```ts
watchedChannels: ["C0123456789"],
```

Then invite atta to the channel (`/invite @atta`) and restart atta.

## Choices to revisit

These were decided without you and are easy to change:

- Only top-level messages and replies in threads atta is not part of are observed. Messages in a thread with an approved worker, from you, still go straight to the worker.
- The router decides what is worth proposing from its instructions alone. There is no rate limit or "already proposed" check beyond asking the router not to propose the same work twice.
- A proposal does not expire. A Start click days later still starts the worker.
- After Start, the worker's result is posted in the channel thread, where the colleague who asked can see it. Replies there from others are observed, not passed to the worker.
- Every observed message costs one observer turn. In a busy channel that is many turns; a cheaper model or a first filter could come later.
- The observer uses the router's model and effort. It could have its own settings.
