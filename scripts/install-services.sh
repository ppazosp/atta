#!/bin/sh
# Installs atta's launchd services for the current user, and starts them.
# Usage: install-services.sh [com.atta.t3] [com.atta.app]   (both when none is given)
set -eu
repo="$(cd "$(dirname "$0")/.." && pwd)"
agents="$HOME/Library/LaunchAgents"
mkdir -p "$agents" "$HOME/.atta/logs"
# Resolve real install folders, not per-shell shims (fnm's multishell folders vanish after a restart).
node_bin="$(dirname "$(realpath "$(command -v node)")")"
# Codex keeps a stable link that follows its updates; use its folder, not the versioned target.
codex_bin="$(dirname "$(command -v codex)")"
git_bin="$(dirname "$(command -v git)")"
service_path="$node_bin:$codex_bin:$git_bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
[ "$#" -gt 0 ] || set -- com.atta.t3 com.atta.app
# Escapes a value for a sed replacement and for XML, so paths with & or | survive.
escape() { printf '%s' "$1" | sed -e 's/&/\&amp;/g' -e 's/[\\|&]/\\&/g'; }
for name in "$@"; do
  sed -e "s|__HOME__|$(escape "$HOME")|g" -e "s|__REPO__|$(escape "$repo")|g" \
    -e "s|__PATH__|$(escape "$service_path")|g" -e "s|__NPX__|$(escape "$node_bin/npx")|g" \
    "$repo/launchd/$name.plist" > "$agents/$name.plist"
  # bootout returns before the old service is gone; wait for it, or bootstrap fails with error 5.
  if launchctl bootout "gui/$(id -u)/$name" 2>/dev/null; then
    while launchctl print "gui/$(id -u)/$name" >/dev/null 2>&1; do sleep 1; done
  fi
  launchctl bootstrap "gui/$(id -u)" "$agents/$name.plist"
  echo "installed and started $name"
done
