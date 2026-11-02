#!/bin/sh
# Prepares T3 Code's data folder for atta (~/.atta/t3): a worker instance that runs your own Codex setup, with
# xhigh reasoning and atta's MCP server added through its launch settings. Leaves existing settings alone.
# Usage: setup-t3.sh [mcp port]   Use the same port as `mcp.port` in your atta config (default 8790).
set -eu
port="${1:-8790}"
settings="$HOME/.atta/t3/userdata/settings.json"
if [ -f "$settings" ]; then
  echo "$settings exists; not changing it"
  exit 0
fi
mkdir -p "$(dirname "$settings")"
cat > "$settings" <<JSON
{
  "defaultRuntimeMode": "full-access",
  "providerInstances": {
    "codex-worker": {
      "driver": "codex",
      "displayName": "atta worker",
      "config": {
        "launchArgs": "-c model_reasoning_effort=\"xhigh\" -c mcp_servers.atta.url=\"http://127.0.0.1:${port}/mcp\""
      }
    }
  }
}
JSON
echo "wrote $settings"
