#!/bin/sh
# Installs atta's two launchd services (T3 Code and atta) for the current user, and starts them.
set -eu
repo="$(cd "$(dirname "$0")/.." && pwd)"
agents="$HOME/Library/LaunchAgents"
mkdir -p "$agents" "$HOME/.atta/logs"
for name in com.atta.t3 com.atta.app; do
  sed -e "s#__HOME__#$HOME#g" -e "s#__REPO__#$repo#g" -e "s#__PATH__#$PATH#g" -e "s#__NPX__#$(command -v npx)#g" \
    "$repo/launchd/$name.plist" > "$agents/$name.plist"
  launchctl bootout "gui/$(id -u)/$name" 2>/dev/null || true
  launchctl bootstrap "gui/$(id -u)" "$agents/$name.plist"
  echo "installed and started $name"
done
