// Copy to atta.config.ts (gitignored) and fill in your own values. Tokens are not here: SLACK_BOT_TOKEN and
// SLACK_APP_TOKEN come from the environment.
export default {
  slack: {
    // Your Slack member ID (profile, then "Copy member ID"). atta acts only on this user's messages.
    trustedUserId: "U0000000000",
  },
  repos: [
    {
      name: "sandbox-repo",
      path: "/Users/you/code/sandbox-repo",
      // The T3 project for this path (T3 app, project settings).
      t3ProjectId: "00000000-0000-0000-0000-000000000000",
    },
  ],
  router: { model: "gpt-6.1-terra", effort: "medium" },
  workers: { model: "gpt-6.1-sol", effort: "xhigh", t3InstanceId: "codex-worker", idleDays: 7 },
  t3: { url: "http://127.0.0.1:3999/mcp" },
  mcp: { port: 8790 },
  schedules: [
    {
      name: "morning",
      cron: "0 9 * * 1-5",
      timezone: "Europe/Madrid",
      // A new thread in a channel each time. Use { thread: { team, conversation, rootTs } } for one thread.
      target: { conversation: { team: "T0000000000", conversation: "C0000000000" } },
      text: "What did my workers finish yesterday, and what is still open?",
    },
  ],
  dataDir: "~/.atta",
}
