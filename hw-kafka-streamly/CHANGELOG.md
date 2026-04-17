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
  helpers built on `Bifunctor` (`mapFirst`, `mapValue`, `bimapValue`).
- `Kafka.Streamly.Sink` — two producer fold variants (`kafkaSink` for
  per-record sends, `kafkaBatchSink` for `[ProducerRecord]` batches) and a
  `withKafkaProducer` bracket helper that flushes and closes on exit.
- `Kafka.Streamly.Combinators` — `batchByOrFlush` and `batchByOrFlushEither`
  for size-bounded batching with explicit flush, and `throwLeft` /
  `throwLeftSatisfy` for raising error values as exceptions.
- Module-level Haddock with worked examples and `@since 0.1.0.0` tags on
  every exported identifier.

### Fixed

- `withKafkaProducer` no longer flushes twice on teardown
  (`Kafka.Producer.closeProducer` already flushes internally).

### Changed

- `batchByOrFlush` and `batchByOrFlushEither` now reject non-positive
  `BatchSize` with an explicit error rather than silently emitting
  singleton batches.
- Value-mapping helpers pruned: the nine `sequenceValue*`, `traverseValue*`,
  and `bitraverseValue*`/`bisequenceValue` variants have been removed.
  Callers who relied on them can use `fmap`/`bimap`/`traverse`/`bitraverse`
  inline. `mapFirst`, `mapValue`, and `bimapValue` remain, re-documented as
  `Bifunctor`/`Functor` lifts.

### Tests

- Initial pure test suite covering error predicates (`isFatal`,
  `isPollTimeout`, `isPartitionEOF`), error filters (`skipNonFatal`,
  `skipNonFatalExcept`), batching combinators (`batchByOrFlush`,
  `batchByOrFlushEither`), and exception combinators (`throwLeft`,
  `throwLeftSatisfy`). Uses `tasty` + `tasty-hunit` + `tasty-quickcheck`.
  Run with `cabal test`.
