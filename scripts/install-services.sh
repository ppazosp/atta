#!/bin/sh
# Installs atta's launchd services for the current user, and starts them.
# Usage: install-services.sh [com.atta.t3] [com.atta.app]   (both when none is given)
# ATTA_INSTANCE=<name> installs a second atta beside the first: service com.atta.app.<name>, with its config,
# secrets and logs in ~/.atta-<name>. T3 is shared, so a named instance installs only com.atta.app.
set -eu
repo="$(cd "$(dirname "$0")/.." && pwd)"
agents="$HOME/Library/LaunchAgents"
instance="${ATTA_INSTANCE:-}"
case "$instance" in
  "") app_label=com.atta.app atta_home="$HOME/.atta" ;;
  *[!a-z0-9-]*) echo "ATTA_INSTANCE may only use a-z, 0-9 and -" >&2; exit 1 ;;
  *) app_label="com.atta.app.$instance" atta_home="$HOME/.atta-$instance" ;;
esac
mkdir -p "$agents" "$atta_home/logs"
# Resolve real install folders, not per-shell shims (fnm's multishell folders vanish after a restart).
node_bin="$(dirname "$(realpath "$(command -v node)")")"
# Codex keeps a stable link that follows its updates; use its folder, not the versioned target.
codex_bin="$(dirname "$(command -v codex)")"
git_bin="$(dirname "$(command -v git)")"
service_path="$node_bin:$codex_bin:$git_bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
if [ "$#" -eq 0 ]; then
  if [ -n "$instance" ]; then set -- com.atta.app; else set -- com.atta.t3 com.atta.app; fi
fi
# Escapes a value for a sed replacement and for XML, so paths with & or | survive.
escape() { printf '%s' "$1" | sed -e 's/&/\&amp;/g' -e 's/[\\|&]/\\&/g'; }
for template in "$@"; do
  name="$template"
  [ "$template" = com.atta.app ] && name="$app_label"
  sed -e "s|__HOME__|$(escape "$HOME")|g" -e "s|__REPO__|$(escape "$repo")|g" \
    -e "s|__PATH__|$(escape "$service_path")|g" -e "s|__NPX__|$(escape "$node_bin/npx")|g" \
    -e "s|__LABEL__|$(escape "$name")|g" -e "s|__ATTA_HOME__|$(escape "$atta_home")|g" \
    "$repo/launchd/$template.plist" > "$agents/$name.plist"
  # bootout returns before the old service is gone; wait for it, or bootstrap fails with error 5.
  if launchctl bootout "gui/$(id -u)/$name" 2>/dev/null; then
    while launchctl print "gui/$(id -u)/$name" >/dev/null 2>&1; do sleep 1; done
  fi
  launchctl bootstrap "gui/$(id -u)" "$agents/$name.plist"
  echo "installed and started $name"
done
