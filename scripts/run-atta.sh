#!/bin/sh
# Starts atta for launchd. Secrets live in $ATTA_HOME/env (mode 0600), never in the plist or the repository.
# ATTA_HOME is the instance's folder: ~/.atta by default, ~/.atta-<instance> for a named instance.
set -eu
cd "$(dirname "$0")/.."
home="${ATTA_HOME:-$HOME/.atta}"
if [ -f "$home/env" ]; then
  set -a
  . "$home/env"
  set +a
fi
export ATTA_CONFIG="${ATTA_CONFIG:-$home/atta.config.ts}"
exec node --experimental-strip-types src/app/main.ts start
