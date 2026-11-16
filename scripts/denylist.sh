#!/usr/bin/env bash
# denylist.sh — fail if the repository names internal tickets, schema, hosts or
# runner internals that do not belong in a public extension. Scans every tracked
# file except go.sum and LICENSE; this script is excluded because it lists them.
set -euo pipefail
cd "$(dirname "$0")/.."

patterns=(
  '\b(LTS|LTAR|LTAI|LTSCLI)-[0-9]+'
  'bstackdemo'
  'blu_test'
  'k6_raw_data_new'
  '\bplu[-_:]'
  'lts[._]metrics|lts\.plu'
  '\bltsReqId\b|\burlFull\b|\blts_data_'
  'entrypoint_k6|k6-test-runner|\brtli-|spawnPods|kafka-clickhouse-shim|k6-req-capturer|pluPodEnvAllowlist'
  '\b(1800m|3400Mi)\b'
  '\bjob[-_ ]?id\s*[:=]\s*[0-9]{5,}'
)

files=()
while IFS= read -r f; do
  case "$f" in go.sum|LICENSE|scripts/denylist.sh) continue ;; esac
  files+=("$f")
done < <(git ls-files --cached --others --exclude-standard)

hits=0
for p in "${patterns[@]}"; do
  if out="$(grep -nEi -- "$p" "${files[@]}" 2>/dev/null)"; then
    echo "$out"
    hits=$((hits + 1))
  fi
done
if [[ "$hits" -gt 0 ]]; then
  echo "denylist: $hits pattern(s) matched" >&2
  exit 1
fi
echo "denylist: clean (${#files[@]} files)"
