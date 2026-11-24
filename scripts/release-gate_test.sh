#!/usr/bin/env bash
# Tests for release-gate.sh against a recorded `go version -m` output
# (testdata/buildinfo-amd64.txt, a linux/amd64 build of extensions.json).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
GATE="$HERE/release-gate.sh"
INFO="$HERE/testdata/buildinfo-amd64.txt"
GOMOD="$HERE/testdata/k6-v2.2.0.mod"
MANIFEST="$HERE/../extensions.json"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
failures=0

expect() { # expect <pass|fail> <name> <gate args...>
  local want="$1" name="$2"; shift 2
  if out="$(bash "$GATE" --k6-gomod "$GOMOD" "$@" 2>&1)"; then got=pass; else got=fail; fi
  if [[ "$got" == "$want" ]]; then echo "ok   $name"; else echo "FAIL $name (got $got)"; echo "$out"; failures=$((failures + 1)); fi
}
edit() { perl -pe "$1" "$INFO" > "$tmp/info.txt"; echo "$tmp/info.txt"; }
manifest() { jq "$1" "$MANIFEST" > "$tmp/manifest.json"; echo "$tmp/manifest.json"; }

expect pass "recorded build matches the manifest" --buildinfo "$INFO" --arch amd64
expect fail "wrong arch" --buildinfo "$INFO" --arch arm64
expect fail "CGO enabled" --buildinfo "$(edit 's/CGO_ENABLED=0/CGO_ENABLED=1/')" --arch amd64
expect fail "built for another OS" --buildinfo "$(edit 's/GOOS=linux/GOOS=darwin/')" --arch amd64
expect fail "a third-party module linked at another version" \
  --buildinfo "$(edit 's|(github.com/grafana/xk6-faker\t)v0.5.3|${1}v0.5.2|')" --arch amd64
expect fail "manifest pins a version the binary does not link" \
  --buildinfo "$INFO" --manifest "$(manifest '(.extensions[] | select(.name == "faker") | .version) = "v0.5.2"')" --arch amd64
expect fail "a third-party module replaced" \
  --buildinfo "$(edit 's|^(\tdep\tgithub.com/grafana/xk6-dns\t.*)$|$1\n\t=>\t../fork\t(devel)\t|')" --arch amd64
expect fail "another k6 version" --buildinfo "$(edit 's|(go.k6.io/k6/v2\t)v2.2.0|${1}v2.2.1|')" --arch amd64
expect fail "manifest k6Version differs" \
  --buildinfo "$INFO" --manifest "$(manifest '.k6Version = "v2.3.0"')" --arch amd64
expect fail "a declared extension missing from the binary" \
  --buildinfo "$(edit '$_ = "" if m|github.com/grafana/xk6-tcp\t|')" --arch amd64
expect fail "the toolkit missing from the binary" \
  --buildinfo "$(edit '$_ = "" if m|\tdep\tgithub.com/browserstack/xk6-load-toolkit\t|')" --arch amd64
expect fail "an undeclared extension linked" \
  --buildinfo "$(edit 's|^(\tdep\tgithub.com/grafana/xk6-tcp\t.*)$|$1\n\tdep\tgithub.com/example/xk6-extra\tv1.0.0\th1:x|')" --arch amd64

[[ "$failures" -eq 0 ]] || { echo "$failures release-gate test(s) failed"; exit 1; }
echo "all release-gate tests passed"
