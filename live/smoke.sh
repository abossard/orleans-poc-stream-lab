#!/usr/bin/env bash
# Smoke test against the running stack (cd live && docker compose up --build):
# publish on each lane's live silo and expect OnNextAsync for that stream on its /api/events feed.
set -euo pipefail

for url in ${LANES:-http://localhost:8102 http://localhost:8103}; do
  key="smoke-$RANDOM"
  feed=$(mktemp)
  curl -sN --max-time 20 "$url/api/events" > "$feed" &
  sleep 1
  curl -sf -X POST "$url/api/publish" -H 'content-type: application/json' -d "{\"key\":\"$key\"}" > /dev/null
  ok=""
  for _ in $(seq 1 30); do
    if jq -Rne --arg k "$key" '[inputs | ltrimstr("data: ") | fromjson? | select(.stream == $k and .kind == "OnNextAsync")] | length > 0' "$feed" > /dev/null; then
      ok=1
      break
    fi
    sleep 0.5
  done
  kill %1 2> /dev/null || true
  rm -f "$feed"
  [[ -n "$ok" ]] || { echo "FAIL $url: no OnNextAsync for $key"; exit 1; }
  echo "ok   $url: $(curl -sf "$url/api/state" | jq -r '"Orleans \(.orleans) \(.transport)"'), OnNextAsync for $key"
done
