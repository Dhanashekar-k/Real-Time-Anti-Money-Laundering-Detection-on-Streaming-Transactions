#!/usr/bin/env bash
# Create the Kafka topics from manual/02_architecture_and_stack.md section 3 (local partition
# counts). Idempotent: a topic that already matches is left alone; a topic that is missing is
# created; a topic whose partitions or retention differ is reported as DRIFT and the script exits
# 1 without changing it (partitions cannot shrink, and a silent retention change would hide a bug).
#
# Run via `make topics` (needs `make up`). Redpanda has auto_create_topics_enabled=false, so a
# producer typo fails instead of creating a stray topic.
set -euo pipefail

BROKERS="${AML_KAFKA_BROKERS:-localhost:19092}"
HOUR=3600000
DAY=$((24 * HOUR))

# name  partitions  retention.ms        (single local broker, so replication factor 1)
TOPICS=(
  "bank_a.raw     6   $((24 * HOUR))"
  "bank_b.raw     6   $((24 * HOUR))"
  "bank_c.raw     3   $((24 * HOUR))"
  "tx.canonical   12  $((48 * HOUR))"
  "tx.dlq         1   $((7 * DAY))"
  "tx.late        1   $((7 * DAY))"
  "tx.features    12  $((24 * HOUR))"
  "tx.scores      12  $((48 * HOUR))"
)

# Tests set AML_TOPICS_SPEC (one "name partitions retention.ms" row per line) to run this whole
# script against a scratch topic, so they never alter a real topic. Unset = the list above.
# (while-read, not mapfile: macOS ships bash 3.2.)
if [[ -n "${AML_TOPICS_SPEC:-}" ]]; then
  TOPICS=()
  while IFS= read -r line; do
    [[ -n "${line// /}" ]] && TOPICS+=("$line")
  done <<<"$AML_TOPICS_SPEC"
fi

rpk_() { rpk "$@" -X "brokers=$BROKERS"; }

if ! existing_json=$(rpk_ topic list --format json 2>&1); then
  echo "cannot reach Kafka at $BROKERS (is the stack up? try: make up)" >&2
  echo "$existing_json" >&2
  exit 1
fi

created=0 unchanged=0 drift=0
for row in "${TOPICS[@]}"; do
  read -r name partitions retention <<<"$row"

  if jq -e --arg n "$name" 'any(.[]; .name == $n)' <<<"$existing_json" >/dev/null; then
    desc=$(rpk_ topic describe "$name" -c --format json)
    have_p=$(jq -r '.[0].summary.partitions' <<<"$desc")
    have_r=$(jq -r '.[0].configs[] | select(.key == "retention.ms") | .value' <<<"$desc")
    if [[ "$have_p" == "$partitions" && "$have_r" == "$retention" ]]; then
      echo "ok       $name (partitions=$have_p, retention.ms=$have_r)"
      unchanged=$((unchanged + 1))
    else
      echo "DRIFT    $name: partitions=$have_p retention.ms=$have_r, expected partitions=$partitions retention.ms=$retention" >&2
      drift=$((drift + 1))
    fi
  else
    rpk_ topic create "$name" -p "$partitions" -r 1 -c "retention.ms=$retention" >/dev/null
    echo "created  $name (partitions=$partitions, retention.ms=$retention)"
    created=$((created + 1))
  fi
done

echo "topics: created=$created unchanged=$unchanged drift=$drift"
[[ "$drift" -eq 0 ]]
