#!/usr/bin/env bash
# Tests for tag-release.sh against throwaway repositories (no network).
set -euo pipefail

SCRIPT="$(cd "$(dirname "$0")" && pwd)/tag-release.sh"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
fails=0
check() { # name expected actual
  if [[ "$2" == "$3" ]]; then echo "ok   $1"; else echo "FAIL $1: expected '$2', got '$3'"; fails=$((fails + 1)); fi
}

git init --quiet --bare "$WORK/remote.git"
git init --quiet "$WORK/repo"
cd "$WORK/repo"
git config user.email test@example.com
git config user.name test
git remote add origin "$WORK/remote.git"
commit() { git commit --quiet --allow-empty -m "$1"; git rev-parse HEAD; }

c1="$(commit one)"
out="$(bash "$SCRIPT" "$c1")"
check "first commit gets .0" "tag-release: tagged $c1 as v0.1.0" "$out"
check "the tag is pushed" "$c1" "$(git --git-dir="$WORK/remote.git" rev-parse v0.1.0)"

out="$(bash "$SCRIPT" "$c1")"
check "re-run keeps the existing tag" "tag-release: $c1 is already v0.1.0" "$out"

c2="$(commit two)"
check "next commit counts up" "tag-release: tagged $c2 as v0.1.1" "$(bash "$SCRIPT" "$c2")"

git tag v0.1.9 "$c1"
git tag v0.1.x "$c1"
git tag v0.2.0 "$c1"
c3="$(commit three)"
check "numeric, series-only max" "tag-release: tagged $c3 as v0.1.10" "$(bash "$SCRIPT" "$c3" --no-push)"

check "rejects a non-commit" "1" "$(bash "$SCRIPT" deadbeef >/dev/null 2>&1; echo $?)"
check "rejects a bad series" "2" "$(bash "$SCRIPT" "$c3" --series 0.1 >/dev/null 2>&1; echo $?)"

[[ "$fails" -eq 0 ]] || { echo "$fails failure(s)"; exit 1; }
echo "all tag-release tests passed"
