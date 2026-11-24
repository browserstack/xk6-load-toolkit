#!/bin/sh
# toolkit-tree-hash.sh <dir> — prints the xk6-load-toolkit tree hash: the sha256
# of the sorted `sha256sum` lines (paths relative to <dir>) of every *.go, go.mod
# and go.sum under <dir>. Identical copies live in the toolkit repository and in
# the backend's runner/xk6-extensions/, so both sides compute the same value.
set -eu
dir="${1:?usage: toolkit-tree-hash.sh <dir>}"
cd "$dir"
find . -type f \( -name '*.go' -o -name go.mod -o -name go.sum \) -not -path './.git/*' \
  | sed 's|^\./||' | LC_ALL=C sort \
  | while IFS= read -r f; do sha256sum "$f"; done \
  | sha256sum | cut -d' ' -f1
