# Runbook: the dead-letter topic (`tx.dlq`)

A record a connector cannot map is not dropped and does not stop the stream: it goes to `tx.dlq` as JSON, and the input offset is committed with the rest of its batch. Nothing re-publishes it automatically. This is how a person looks at it and, when the cause was a mapping gap, sends it through again. Every command below was run against scratch topics on 2026-11-23 (M3). `--offset :end` makes `rpk` read to the current end and exit; without it (or `--num`) `rpk topic consume` waits for new messages forever.

## What a DLQ record holds

```json
{"raw": "<the message text>", "error_code": "UNKNOWN_VALUE",
 "error_detail": "payment_format: 'HAWALA' is not in value map 'method_names'",
 "source_topic": "bank_b.raw", "partition": 3, "offset": 1042, "mapping_version": "b-1"}
```

The Kafka key is the original key (the sender account key). `raw` is the text as received; bytes that were not UTF-8 are escaped (`\xff`), so such a record can be read but not replayed byte for byte. A record that carried a field the bank should not send is carried as is: the DLQ is not filtered.

Codes: `MALFORMED_RECORD` (not parseable, wrong shape or column count), `MISSING_FIELD`, `UNKNOWN_VALUE` (a vocabulary value or currency the bank file does not know), `BAD_NUMBER`, `BAD_TIMESTAMP`, `TIMESTAMP_OUT_OF_RANGE`, `NEGATIVE_AMOUNT`, `BAD_FORMAT`, `INTERNAL_ERROR` (an engine bug: report it). See `connectors/README.md`.

## Who looks, and when

The connector exports `dlq_total{bank, error_code}` (Prometheus, port 18101 for Bank A and 18102 for Bank B when started with `make connectors`). Any non-zero value is worth reading; a rising one right after a new bank or a bank-side change is a mapping gap. No alert rule exists yet (M8).

## 1. Look

```bash
rpk topic consume tx.dlq -X brokers=localhost:19092 --offset :end -f '%v\n' | jq -r .error_code | sort | uniq -c
rpk topic consume tx.dlq -X brokers=localhost:19092 --offset :end --num 5 -f '%v\n' | jq .
# which values does the bank send that the file does not know?
rpk topic consume tx.dlq -X brokers=localhost:19092 --offset :end -f '%v\n' \
  | jq -r 'select(.error_code=="UNKNOWN_VALUE") | .error_detail' | sort | uniq -c
```

## 2. Decide

- A vocabulary value the bank really uses (`UNKNOWN_VALUE`): add it to `value_maps` in `connectors/banks/bank_<x>.yaml` (or ask the bank what it means).
- A value that is wrong in the data (a negative amount, a time in 2030): not a mapping gap. Leave it in the DLQ and tell the bank.
- `MALFORMED_RECORD`: look at `raw`; a bank-side bug or a truncated message.

## 3. Check the fix before sending anything again

Dump the raw texts to a file and run the dry-run tool on them: it needs no Kafka and shows whether the fixed file now maps them.

```bash
rpk topic consume tx.dlq -X brokers=localhost:19092 --offset :end -f '%v\n' \
  | jq -r 'select(.source_topic=="bank_b.raw") | .raw' > /tmp/bank_b_dlq.jsonl
make onboard-check BANK=b SAMPLE=/tmp/bank_b_dlq.jsonl     # exit 0 = nothing would go to the DLQ
```

## 4. Restart the connector with the new file

Bump `mapping_version` in the YAML (`b-1` to `b-2`) so records can be told apart downstream, then `make connectors` (rebuilds the image and recreates the container). Records the connector already mapped keep their old `mapping_version`.

## 5. Re-publish

Send each raw text, with its key, back to the **source topic** (`source_topic` in the record), where the connector picks it up like any new message. Records keep their `tx_id`, so Flink's dedupe by `tx_id` (D15) drops a transaction that somehow arrives twice.

```bash
rpk topic consume tx.dlq -X brokers=localhost:19092 --offset :end -f '%k\t%v\n' \
  | while IFS=$'\t' read -r key value; do
      printf '%s\t%s\n' "$key" "$(printf '%s' "$value" | jq -r .raw)"
    done \
  | rpk topic produce bank_b.raw -X brokers=localhost:19092 -f '%k\t%v\n'
```

Replace `bank_b.raw` by the record's `source_topic`; add a `jq` filter on `.source_topic` first when the DLQ holds both banks. The `replay_emit_ts_ms` header is not re-created, so a re-published record has none and its ingest latency is not measured.

Re-published records are new messages at the end of their partition, so they arrive after later transactions of the same account, out of event-time order. M4 (not built yet) is designed to send records later than its out-of-orderness bound to `tx.late` instead of into the features; check that behaviour once it exists before re-publishing old records.

The original DLQ records stay in the topic (7 days of retention). Do not delete them to "clear" the alert; the counter is cumulative and only a connector restart resets it.

## Limits

- No tool re-publishes for you; the recipe above is manual on purpose (a person decides).
- Exactly-once is not claimed: a re-published message is a new message.
- The topic is not partitioned by bank (1 partition locally); `source_topic` says which bank.
