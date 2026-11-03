# Watched channels

atta can read channels you add it to, and propose work from what it reads. You approve every worker in a direct message before it starts. Only you can steer atta, by mentioning it.

## Who reaches atta

| Where                                                    | You, with a mention                                                       | You, without a mention         | Anyone else                    |
| -------------------------------------------------------- | ------------------------------------------------------------------------- | ------------------------------ | ------------------------------ |
| A watched channel, or a thread there atta is not part of | A request: it goes to the router, or steers the thread's worker           | Information, in the next batch | Information, in the next batch |
| A thread where atta works                                | A request: it steers the worker, or goes to the router if none is running | Does not reach atta            | Does not reach atta            |

Also:

- in a direct message, everything you write is a request
- in a channel atta is in but does not watch, only your mentions count
- "stop" from you stops a thread's running workers. In a channel it needs a mention, so a stray "stop" in conversation does not end work
- to bring a thread's other messages in, mention atta ("look at this"): the router reads the thread with `read_conversation`

## How it works

1. You add atta to a channel and put the channel ID in `watchedChannels` in `atta.config.ts`.
2. Messages that are information go to the router in batches. When the router is free, a batch goes at once. While it works on one, new messages wait, then all go in the next batch, up to 50 at a time. Each batch is a turn of its own, never mixed with your requests.
3. The router reads the batch as quoted text, with a note that it is information, not a request. Most batches need nothing, so it ends its turn.
4. When something is worth doing, the router calls `start_worker` with a title, a reason and a brief.
5. You get a direct message with the title, the reason, the whole brief, the repository, a link to the thread, and Start and Skip buttons.
6. On Start, atta starts the worker in the thread the work came from. Its plan and result appear there. On Skip, nothing runs. Either way the buttons are replaced by the outcome, and the router hears your decision, so it remembers.

`start_worker` works this way for every worker, including work you ask for in a direct message.

## What stays true

These rules hold in code and are covered by tests in `test/core.test.ts`, `src/core/domain/routing.test.ts` and `src/adapters/pi-durable/index.test.ts`:

- no worker starts without your click, and you see the whole brief first (a brief over 2,000 characters is refused)
- in a turn a channel batch started, `reply`, `message_worker`, `stop_worker` and `subscribe` refuse: the router can only propose. Each tool call carries the newest input the model had read, and only an input atta wrote for a trusted event counts as a trusted turn
- your requests are never steered into a channel turn: they queue, and run as their own turn
- someone else's message never reaches a worker, and in a thread where atta works, nothing but your mention does
- atta posts only in threads you started with it or approved work in, or where a schedule says
- only your click counts; anyone else's is dropped
- a replayed `start_worker` call sends one direct message, and a replayed decision starts one worker

## What it does not stop

Text from a channel stays in the router's memory after the batch that brought it. A later turn you start could write a brief or a message to a worker shaped by that text. Every brief still passes your click, so the gap is a message to a worker that is already running. Running workers in a sandbox closes it.

## Setting it up

Add the channel IDs to your config. The Slack app already has the scopes and events it needs (`channels:history`, `groups:history`, `message.channels`, `message.groups`, `im:write` and interactivity), so you do not need to reinstall it.

```ts
watchedChannels: ["C0123456789"],
```

Then invite atta to the channel (`/invite @atta`) and restart atta.

## Choices to revisit

- A batch waits only for the router's current channel turn. There is no quiet-period timer. Each channel turn is logged with its message count, outcome and duration, to decide whether one is needed.
- Channel turns use the router's model and effort. A cheaper model could read batches.
- A proposal does not expire. A Start click days later still starts the worker.
