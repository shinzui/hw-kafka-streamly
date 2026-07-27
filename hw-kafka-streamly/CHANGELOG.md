# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to the [Haskell Package Versioning Policy](https://pvp.haskell.org/).

## Unreleased

### Added

- `withKafkaConsumerStream` and `withKafkaConsumerStreamOn`, scope-based entry
  points that hand a stream to a continuation and **close the consumer when the
  continuation returns** — however much of the stream was consumed, and even if
  it throws. These are now the recommended way to consume, and the internal
  `withConsumerStreamVia` is exported so the guarantee can be tested.

### Fixed

- The module's worked example, and every example in
  `hw-kafka-streamly-jitsurei`, no longer leak a consumer. All of them took a
  fixed number of records and then abandoned the stream, which is exactly the
  case `kafkaStream` cannot clean up promptly, so every example a user might
  copy demonstrated the leak. They now use `withKafkaConsumerStream`.

### Changed

- `kafkaStream` and `kafkaStreamAutoClose` carry a prominent warning that their
  close is deferred to the garbage collector when the stream is partially
  consumed and abandoned — a `Stream.take`, an early-terminating fold, or an
  exception thrown downstream. This is upstream-documented streamly behaviour
  (*"Worst case … cleanup is deferred to GC: the bracketed stream is partially
  consumed and abandoned"*), not a defect in this library, but it is far more
  consequential for a Kafka consumer than for an ordinary resource: until some
  GC runs, hw-kafka-client's background loop keeps polling, which keeps the
  group membership alive, keeps the partitions assigned to a consumer nobody is
  reading, and keeps resetting librdkafka's `max.poll.interval.ms` progress
  watchdog — so those partitions are starved with no rebalance. A quiet process
  may never run that GC, and process exit runs no finalizers at all. Neither
  function is deprecated: both are correct when the stream is drained.

- `isFatal` now classifies `RdKafkaRespErrFatal` as fatal. This is the generic
  code librdkafka uses to report that a fatal error has been raised on the
  client — most importantly a __fenced__ static group member, where a second
  consumer joined with the same `group.instance.id`. After a fatal error
  librdkafka permanently halts all consumer group activity, so the consumer is
  dead and cannot recover. Because `isFatal` previously fell through to its
  catch-all and returned `False`, the recommended `skipNonFatal` filter
  silently *discarded* that signal and the stream went on polling a dead
  consumer forever. Any consumer relying on `skipNonFatal` to surface fatal
  conditions was affected.

- `isFatal` now classifies `RdKafkaRespErrSaslAuthenticationFailed` as fatal.
  This is the broker-side SASL rejection; like the already-fatal transport-level
  `RdKafkaRespErrAuthentication`, retrying it in a tight poll loop never helps
  and can lock accounts.

### Changed

- `cabal.project` pins the compiler to `ghc-9.12.4`, matching the version this
  repository's own Nix devShell provides (`nix/haskell.nix`). The previous
  `ghc-9.12.2` pin no longer resolved to an installed compiler and made the
  project unbuildable.

- `cabal.project` pins `hw-kafka-client` to a patched fork
  (`shinzui/hw-kafka-client`) that surfaces consumer fatal errors in
  `CallbackPollModeAsync` and stops leaking every message its background
  callback-poll loop consumes. Without this, a fatal error is unobservable in
  async mode at any layer, so the `isFatal` fix above only takes effect in
  `CallbackPollModeSync`. The pin is by commit and is intended to last until the
  change is available upstream.

## 0.2.0.0 — 2026-05-07

### Changed (BREAKING)

- Renamed modules and functions to use streamly's vocabulary instead of
  conduit's. The functions return Streamly `Stream`s and `Fold`s, so the
  modules are now named `Kafka.Streamly.Stream` and `Kafka.Streamly.Fold`,
  and the function families are `kafkaStream*` and `kafkaFold*`.

  | Old (0.1.0.0)                  | New (0.2.0.0)                |
  |--------------------------------|------------------------------|
  | `Kafka.Streamly.Source`        | `Kafka.Streamly.Stream`      |
  | `Kafka.Streamly.Sink`          | `Kafka.Streamly.Fold`        |
  | `kafkaSource`                  | `kafkaStream`                |
  | `kafkaSourceAutoClose`         | `kafkaStreamAutoClose`       |
  | `kafkaSourceNoClose`           | `kafkaStreamNoClose`         |
  | `kafkaSink`                    | `kafkaFold`                  |
  | `kafkaBatchSink`               | `kafkaBatchFold`             |

  No backward-compatible re-exports are provided. Callers porting from
  0.1.0.0 should mechanically substitute identifiers per the table.

## 0.1.0.0 — 2026-04-17

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
