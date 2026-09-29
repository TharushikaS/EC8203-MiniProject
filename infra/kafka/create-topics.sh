#!/usr/bin/env bash
# Idempotent topic creation. Partition counts are explicit design decisions:
#   meter-readings      6 partitions, keyed by household_id -> per-meter ordering, parallel consumption
#   meter-readings-dlq  1 partition  (low volume, ordered for inspection)
#   grid-alerts         3 partitions, keyed by grid_zone
# Retention 7 days on the source topic gives a replay window for the speed layer; the batch
# layer's long-term history lives in the Parquet raw archive, not in Kafka.
set -euo pipefail
BS=kafka:9092
K=/opt/kafka/bin/kafka-topics.sh

create() {
  "$K" --bootstrap-server "$BS" --create --if-not-exists --topic "$1" \
       --partitions "$2" --replication-factor 1 --config retention.ms="$3" --config compression.type=producer
}

create meter-readings     6 604800000
create meter-readings-dlq 1 1209600000
create grid-alerts        3 604800000

"$K" --bootstrap-server "$BS" --describe --topic meter-readings
echo "topics ready"
