#!/usr/bin/env bash
# build.sh — build a k6 binary with every extension listed in a manifest.
#
# Usage:
#   ./build.sh [--manifest <path>] [--repo-root <path>] [--k6-version <ver>] [--output <path>]
#
# Defaults: --manifest ./extensions.json, --repo-root the directory of this
# script, --k6-version the manifest's "k6Version", --output ./dist/k6.
# GOOS / GOARCH from the environment select the target; CGO is always off.
#
# Each manifest entry has exactly one of:
#   "repoPath": a directory relative to --repo-root  -> --with <module>=<dir>
#   "version":  a module version                     -> --with <module>@<version>
# An entry with both, or neither, fails the build: a pin must never be ignored.
#
# Requires xk6, jq and go on PATH.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$SCRIPT_DIR"
MANIFEST="$SCRIPT_DIR/extensions.json"
OUTPUT="$SCRIPT_DIR/dist/k6"
K6_VERSION=""

for arg in "$@"; do
  case "$arg" in
    -h|--help) grep -E '^# ' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
  esac
done

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-root)  REPO_ROOT="$2"; shift 2 ;;
    --k6-version) K6_VERSION="$2"; shift 2 ;;
    --output)     OUTPUT="$2"; shift 2 ;;
    --manifest)   MANIFEST="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

[[ -f "$MANIFEST" ]] || { echo "Manifest not found: $MANIFEST" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "jq is required to parse the manifest" >&2; exit 1; }
command -v xk6 >/dev/null 2>&1 || { echo "xk6 is not on PATH; install with: go install go.k6.io/xk6/cmd/xk6@<version>" >&2; exit 1; }

if [[ -z "$K6_VERSION" ]]; then
  K6_VERSION="$(jq -r '.k6Version // ""' "$MANIFEST")"
fi
[[ -n "$K6_VERSION" ]] || { echo "No k6 version: pass --k6-version or set k6Version in the manifest" >&2; exit 1; }

WITH_ARGS=()
# "|" rather than tab: tab is IFS whitespace, so an empty field would collapse
# and shift the later columns, silently turning a version into a path.
while IFS='|' read -r name module repo_path version; do
  if [[ -n "$repo_path" && -n "$version" ]]; then
    echo "[build] ERROR: extension '$name' sets both repoPath and version; the pin would be ignored" >&2
    exit 1
  fi
  if [[ -n "$repo_path" ]]; then
    ext_path="$(cd "$REPO_ROOT" && cd "$repo_path" 2>/dev/null && pwd)" || {
      echo "[build] ERROR: extension '$name' source missing at $REPO_ROOT/$repo_path" >&2; exit 1; }
    echo "[build] + $name  ($module)  <- $ext_path"
    WITH_ARGS+=(--with "${module}=${ext_path}")
  else
    if [[ -z "$version" ]]; then
      echo "[build] ERROR: extension '$name' has neither repoPath nor version" >&2
      exit 1
    fi
    echo "[build] + $name  (${module}@${version})"
    WITH_ARGS+=(--with "${module}@${version}")
  fi
done < <(jq -r '.extensions[] | [.name, .module, (.repoPath // ""), (.version // "")] | join("|")' "$MANIFEST")

[[ ${#WITH_ARGS[@]} -gt 0 ]] || { echo "[build] manifest is empty, nothing to build" >&2; exit 1; }

mkdir -p "$(dirname "$OUTPUT")"
echo "[build] k6=${K6_VERSION} GOOS=${GOOS:-host} GOARCH=${GOARCH:-host} output=${OUTPUT} extensions=$((${#WITH_ARGS[@]} / 2))"

# xk6 reads K6_VERSION from the environment as its --k6-version flag, so pass
# the version only as the flag, never also as a positional argument.
# Module-proxy fetches fail transiently; retry, resuming from the module cache.
attempt=1
until CGO_ENABLED=0 xk6 build --verbose --k6-version "$K6_VERSION" "${WITH_ARGS[@]}" --output "$OUTPUT"; do
  if [[ "$attempt" -ge 3 ]]; then
    echo "[build] FAILED after ${attempt} attempts" >&2
    exit 1
  fi
  echo "[build] attempt ${attempt}/3 failed; retrying in $((attempt * 15))s" >&2
  sleep $((attempt * 15))
  attempt=$((attempt + 1))
done

echo "[build] done -> $OUTPUT"
