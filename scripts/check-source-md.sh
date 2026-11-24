#!/usr/bin/env bash
# check-source-md.sh — fail when SOURCE.md no longer lists what the build uses:
# the manifest's k6 version, every pinned extension at its version, and the Go
# version CI builds with.
#
#   scripts/check-source-md.sh [--manifest extensions.json] [--source SOURCE.md] [--ci .github/workflows/ci.yml]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MANIFEST="$ROOT/extensions.json"
SOURCE="$ROOT/SOURCE.md"
CI="$ROOT/.github/workflows/ci.yml"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --manifest) MANIFEST="$2"; shift 2 ;;
    --source) SOURCE="$2"; shift 2 ;;
    --ci) CI="$2"; shift 2 ;;
    *) echo "check-source-md: unknown argument $1" >&2; exit 2 ;;
  esac
done

errors=0
fail() { echo "check-source-md: $*" >&2; errors=$((errors + 1)); }

# A row matches when it names the module in backticks and the version as a cell.
row_has() { grep -F -- "\`$1\`" "$SOURCE" | grep -qF -- "| $2 |"; }

k6_version="$(jq -r '.k6Version' "$MANIFEST")"
row_has "go.k6.io/k6/v2" "$k6_version" || fail "k6 $k6_version is not listed"

while IFS='|' read -r module version; do
  row_has "$module" "$version" || fail "$module $version is not listed"
done < <(jq -r '.extensions[] | select(.version) | "\(.module)|\(.version)"' "$MANIFEST")

listed="$(grep -cE '^\| [a-z0-9-]+ \| `github\.com/grafana/xk6-' "$SOURCE" || true)"
pinned="$(jq '[.extensions[] | select(.version)] | length' "$MANIFEST")"
[[ "$listed" -eq "$pinned" ]] || fail "lists $listed extensions, the manifest pins $pinned"

go_version="$(sed -nE 's/.*go-version: "([0-9.]+)".*/\1/p' "$CI" | head -1)"
[[ -n "$go_version" ]] || fail "no go-version in $CI"
grep -qF "go${go_version}," "$SOURCE" || fail "Go toolchain go${go_version} (from $CI) is not listed"

[[ "$errors" -eq 0 ]] || exit 1
echo "check-source-md: SOURCE.md matches the manifest and go${go_version}"
