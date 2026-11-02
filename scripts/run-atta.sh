#!/bin/sh
# Starts atta for launchd. Secrets live in ~/.atta/env (mode 0600), never in the plist or the repository.
set -eu
cd "$(dirname "$0")/.."
if [ -f "$HOME/.atta/env" ]; then
  set -a
  . "$HOME/.atta/env"
  set +a
fi
export ATTA_CONFIG="${ATTA_CONFIG:-$HOME/.atta/atta.config.ts}"
exec node --experimental-strip-types src/app/main.ts start
