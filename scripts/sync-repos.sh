#!/bin/sh
# Clones read-only source references into .repos/ (gitignored) at the exact versions Atta pins.
# Agents read them for patterns; nothing imports from them.
set -eu
cd "$(dirname "$0")/.."
mkdir -p .repos

clone() { # name url ref
  if [ -d ".repos/$1/.git" ]; then
    git -C ".repos/$1" fetch --depth 1 origin "refs/tags/$3:refs/tags/$3" && git -C ".repos/$1" checkout -q "$3"
  else
    git clone --quiet --depth 1 --branch "$3" "$2" ".repos/$1"
  fi
  echo "$1 at $3"
}

clone effect https://github.com/Effect-TS/effect.git effect@4.0.7
clone pi https://github.com/earendil-works/pi.git v1.5.0
