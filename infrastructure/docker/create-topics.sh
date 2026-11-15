#!/usr/bin/env bash
set -euo pipefail

bootstrap="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
topics=(transactions.raw transactions.validated transactions.rejected transactions.enriched fraud.alerts transactions.dead-letter)
declare -A retention_ms=(
  [transactions.raw]=604800000
  [transactions.validated]=604800000
  [transactions.rejected]=1209600000
  [transactions.enriched]=604800000
  [fraud.alerts]=2592000000
  [transactions.dead-letter]=2592000000
)

for topic in "${topics[@]}"; do
  kafka-topics --bootstrap-server "$bootstrap" --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1 --config "retention.ms=${retention_ms[$topic]}"
done

echo "Topic initialization completed. Review topics.yaml before changing partitioning or retention."
