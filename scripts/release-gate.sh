#!/usr/bin/env bash
# release-gate.sh — prove a built k6 binary is what extensions.json declares,
# from the binary's own Go build info. Run on every binary before it is released.
#
#   scripts/release-gate.sh --binary <k6> --arch amd64|arm64 [--manifest extensions.json]
#   scripts/release-gate.sh --buildinfo <file> --arch ...   (pre-captured `go version -m` output)
#
# Fails unless: GOOS=linux, GOARCH=<arch> and CGO_ENABLED=0; go.k6.io/k6/v2 is
# linked at the manifest's k6Version; every "version" entry is linked at exactly
# that version and not replaced; every "repoPath" entry is linked; and no other
# xk6-* module is linked, except ones k6 itself requires (read from k6's go.mod
# via `go mod download`, or from --k6-gomod).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MANIFEST="$SCRIPT_DIR/../extensions.json"
BINARY=""; BUILDINFO=""; ARCH=""; K6_GOMOD=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --binary) BINARY="$2"; shift 2 ;;
    --buildinfo) BUILDINFO="$2"; shift 2 ;;
    --arch) ARCH="$2"; shift 2 ;;
    --manifest) MANIFEST="$2"; shift 2 ;;
    --k6-gomod) K6_GOMOD="$2"; shift 2 ;;
    *) echo "release-gate: unknown argument $1" >&2; exit 2 ;;
  esac
done
[[ -n "$ARCH" ]] || { echo "release-gate: --arch is required" >&2; exit 2; }
[[ -f "$MANIFEST" ]] || { echo "release-gate: manifest not found: $MANIFEST" >&2; exit 2; }

if [[ -n "$BUILDINFO" ]]; then
  info="$(cat "$BUILDINFO")"
else
  [[ -f "$BINARY" ]] || { echo "release-gate: binary not found: $BINARY" >&2; exit 2; }
  info="$(go version -m "$BINARY")"
fi

errors=0
fail() { echo "release-gate: FAIL: $*" >&2; errors=$((errors + 1)); }

setting() { printf '%s\n' "$info" | awk -F'\t' -v k="$1" '$2 == "build" && index($3, k "=") == 1 { print substr($3, length(k) + 2) }'; }
# "<module> <version> <replaced:0|1>" for every linked dependency.
deps="$(printf '%s\n' "$info" | awk -F'\t' '
  $2 == "dep" { if (m != "") print m, v, r; m = $3; v = $4; r = 0; next }
  $2 == "=>"  { r = 1; next }
  END { if (m != "") print m, v, r }')"
dep_line() { printf '%s\n' "$deps" | awk -v m="$1" '$1 == m { print; exit }'; }

[[ "$(setting GOOS)" == "linux" ]] || fail "GOOS is '$(setting GOOS)', want linux"
[[ "$(setting GOARCH)" == "$ARCH" ]] || fail "GOARCH is '$(setting GOARCH)', want $ARCH"
[[ "$(setting CGO_ENABLED)" == "0" ]] || fail "CGO_ENABLED is '$(setting CGO_ENABLED)', want 0"

k6_want="$(jq -r '.k6Version // ""' "$MANIFEST")"
k6_line="$(dep_line go.k6.io/k6/v2)"
[[ -n "$k6_want" ]] || fail "manifest has no k6Version"
[[ "$k6_line" == "go.k6.io/k6/v2 $k6_want 0" ]] || fail "go.k6.io/k6/v2 linked as '${k6_line:-<missing>}', want $k6_want (not replaced)"

if [[ -z "$K6_GOMOD" && -n "$k6_want" ]]; then
  K6_GOMOD="$(go mod download -json "go.k6.io/k6/v2@$k6_want" | jq -r '.GoMod // ""')" || true
fi
[[ -f "$K6_GOMOD" ]] || fail "could not read go.mod of go.k6.io/k6/v2@$k6_want"
k6_requires=" $(awk '$1 == "require" && $2 != "(" { print $2 } /^require \(/ { inblock = 1; next } inblock && $1 == ")" { inblock = 0 } inblock { print $1 }' "${K6_GOMOD:-/dev/null}" 2>/dev/null | tr '\n' ' ')"

declared=" "
while IFS='|' read -r module version repo_path; do
  declared+="$module "
  line="$(dep_line "$module")"
  if [[ -n "$version" ]]; then
    [[ "$line" == "$module $version 0" ]] || fail "$module linked as '${line:-<missing>}', want $version (not replaced)"
  elif [[ -n "$repo_path" ]]; then
    [[ -n "$line" ]] || fail "$module (repoPath $repo_path) is not linked"
  fi
done < <(jq -r '.extensions[] | [.module, (.version // ""), (.repoPath // "")] | join("|")' "$MANIFEST")

while read -r module _; do
  case "$module" in */xk6-*) ;; *) continue ;; esac
  [[ "$declared" == *" $module "* || "$k6_requires" == *" $module "* ]] || fail "$module is linked but not declared in the manifest"
done <<< "$deps"

if [[ "$errors" -gt 0 ]]; then
  echo "release-gate: $errors check(s) failed for $ARCH" >&2
  exit 1
fi
echo "release-gate: OK ($ARCH, k6 $k6_want, $(jq '.extensions | length' "$MANIFEST") extensions)"
