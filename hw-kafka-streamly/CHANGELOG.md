# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to the [Haskell Package Versioning Policy](https://pvp.haskell.org/).

## 0.1.0.0 (unreleased)

Initial release.

### Added

- `Kafka.Streamly.Source` — three consumer stream variants (`kafkaSource`,
  `kafkaSourceAutoClose`, `kafkaSourceNoClose`) for different resource-ownership
  models; error predicates (`isFatal`, `isPollTimeout`, `isPartitionEOF`);
  error filters (`skipNonFatal`, `skipNonFatalExcept`); and value-mapping
  helpers built on `Bifunctor`/`Traversable`/`Bitraversable`
  (`mapFirst`, `mapValue`, `bimapValue`, `sequenceValueFirst`, `sequenceValue`,
  `bisequenceValue`, `traverseValueFirst`, `traverseValue`, `bitraverseValue`,
  `traverseValueFirstM`, `traverseValueM`, `bitraverseValueM`).
- `Kafka.Streamly.Sink` — two producer fold variants (`kafkaSink` for
  per-record sends, `kafkaBatchSink` for `[ProducerRecord]` batches) and a
  `withKafkaProducer` bracket helper that flushes and closes on exit.
- `Kafka.Streamly.Combinators` — `batchByOrFlush` and `batchByOrFlushEither`
  for size-bounded batching with explicit flush, and `throwLeft` /
  `throwLeftSatisfy` for raising error values as exceptions.
