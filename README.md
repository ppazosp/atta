# Atta

Send work to coding agents from Slack and follow it there.

You write to Atta in Slack. A lean router decides what to do: answer you, ask a question, or start a worker. Workers are coding agents running in [T3 Code](https://github.com/pingdotgg/t3code) on your own subscription. You watch their steps in the Slack thread, reply to steer them, and get the result in the same thread.

Atta is named after leafcutter ants, whose colonies split the work between castes.

Status: being built. Nothing runs yet. See [the v0 plan](docs/v0-plan.md) for the design and the order of work.

## How it fits together

![Atta architecture](docs/architecture.png)

- Channels (Slack now, GitHub and email later) turn messages into events.
- The router runs on [Pi Durable](https://github.com/earendil-works/pi) with no shell, only Atta's tools.
- Workers run in T3 Code with your own Codex setup, and call back through Atta's MCP server.
- Everything is stored in SQLite on your machine.

## Development

You need Node 24 and pnpm 12.

```bash
pnpm install
pnpm sync:repos   # read-only Effect and Pi sources for reference, in .repos/
pnpm check        # format, lint, types, tests, unused code
```

Contributor and agent guidelines are in [AGENTS.md](AGENTS.md).

## License

[MIT](LICENSE)
