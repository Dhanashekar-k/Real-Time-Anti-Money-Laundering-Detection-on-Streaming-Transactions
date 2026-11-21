# stream/

Streaming side.

- `replayer/`: replays transactions into the bank topics (added in M3)
- `connectors/`: config-driven connectors, bank formats to `tx.canonical` (M3)
- `flink-features/`: Flink/Java event-time features, package `com.streamaml.features` (M4)

`is_laundering`, pattern data and `split` never enter Kafka topics.
