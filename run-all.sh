#!/usr/bin/env bash
# Runs every scenario x Orleans version x transport and writes results/.
#   ./run-all.sh                                   # memory + eventhub, 10.2.1 + 10.3.1
#   TRANSPORTS=memory ./run-all.sh                 # skip the emulator
#   VERSIONS=10.2.1 SCENARIOS=A,B ./run-all.sh
set -euo pipefail
cd "$(dirname "$0")"

VERSIONS="${VERSIONS:-10.2.1 10.3.1}"
TRANSPORTS="${TRANSPORTS:-memory eventhub}"
SCENARIOS="${SCENARIOS:-A,B,C,D}"

if [[ " $TRANSPORTS " == *" eventhub "* ]]; then
  docker info >/dev/null 2>&1 || { echo "Docker daemon not running (open -a Docker)"; exit 1; }
  docker compose up -d
  echo "waiting for the Event Hubs emulator..."
  for _ in $(seq 1 60); do
    docker logs poc4006-eventhubs-emulator 2>&1 | grep -q "Emulator Service is Successfully Up" && break
    sleep 3
  done
  docker logs poc4006-eventhubs-emulator 2>&1 | grep -q "Emulator Service is Successfully Up" || { echo "emulator did not start"; exit 1; }
fi

mkdir -p results
status=0
for v in $VERSIONS; do
  dotnet build src/Poc -c Release -p:OrleansVersion="$v" -nologo -v q
  for t in $TRANSPORTS; do
    dotnet "artifacts/$v/bin/Release/net10.0/Poc.dll" --transport "$t" --scenarios "$SCENARIOS" --out results || status=1
  done
done

dotnet "artifacts/${VERSIONS##* }/bin/Release/net10.0/Poc.dll" --summary --out results
exit $status
