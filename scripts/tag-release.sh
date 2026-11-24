#!/usr/bin/env bash
# tag-release.sh — give a commit the next release tag in a series (v0.1.0, v0.1.1, ...).
#
#   scripts/tag-release.sh <commit> [--series v0.1] [--remote origin] [--no-push]
#
# Run by .github/workflows/tag-main.yml for every commit that lands on main, so each
# build of main can name its source by tag. Idempotent: a commit that already has a
# tag in the series keeps it. Tags are lightweight and never moved or deleted.
set -euo pipefail

COMMIT="${1:-}"
[[ -n "$COMMIT" ]] || { echo "usage: tag-release.sh <commit> [--series vX.Y] [--remote name] [--no-push]" >&2; exit 2; }
shift
SERIES="v0.1"
REMOTE="origin"
PUSH=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --series) SERIES="$2"; shift 2 ;;
    --remote) REMOTE="$2"; shift 2 ;;
    --no-push) PUSH=0; shift ;;
    *) echo "tag-release: unknown argument $1" >&2; exit 2 ;;
  esac
done
[[ "$SERIES" =~ ^v[0-9]+\.[0-9]+$ ]] || { echo "tag-release: series must look like v0.1" >&2; exit 2; }

sha="$(git rev-parse --verify --quiet "${COMMIT}^{commit}")" || { echo "tag-release: $COMMIT is not a commit" >&2; exit 1; }

if [[ "$PUSH" -eq 1 ]]; then
  git fetch --quiet --tags "$REMOTE"
fi

existing="$(git tag --points-at "$sha" --list "${SERIES}.*" | grep -E "^${SERIES//./\\.}\.[0-9]+$" | head -1 || true)"
if [[ -n "$existing" ]]; then
  echo "tag-release: $sha is already $existing"
  exit 0
fi

last="$(git tag --list "${SERIES}.*" | sed -nE "s/^${SERIES//./\\.}\.([0-9]+)$/\1/p" | sort -n | tail -1)"
next="${SERIES}.$(( ${last:--1} + 1 ))"

git tag "$next" "$sha"
if [[ "$PUSH" -eq 1 ]]; then
  git push --quiet "$REMOTE" "refs/tags/${next}"
fi
echo "tag-release: tagged $sha as $next"
