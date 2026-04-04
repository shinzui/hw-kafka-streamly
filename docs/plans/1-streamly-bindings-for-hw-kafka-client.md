# Streamly bindings for hw-kafka-client

Intention: intention_01knbcpkxqemdaawn3zzs822f8

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

After this work is complete a Haskell developer can consume messages from Apache Kafka and produce messages to Apache Kafka using Streamly streams and folds, getting composable, high-performance streaming pipelines with safe resource management. The developer imports `Kafka.Streamly.Source` to get an infinite `Stream IO (Either KafkaError (ConsumerRecord ...))` from a Kafka topic, applies ordinary Streamly combinators (filter, map, take, fold), and drives a producer via `Kafka.Streamly.Sink`. A companion jitsurei package provides runnable cookbook examples (producer, consumer, error handling, transforms, batching, ETL, concurrency) that exercise every public API surface against a local Redpanda cluster, so the developer can see each pattern working end-to-end before adopting it.


## Progress

- [x] Milestone 1: Project scaffolding and build infrastructure (2026-04-04)
  - [x] Create `cabal.project` with both packages and GHC 9.12 configuration
  - [x] Create `hw-kafka-streamly/hw-kafka-streamly.cabal` with library stanza
  - [x] Create placeholder source modules so the library compiles empty
  - [x] Create `hw-kafka-streamly-jitsurei/hw-kafka-streamly-jitsurei.cabal` with library and executable stanzas
  - [x] Create jitsurei Config module and placeholder Main
  - [x] Update `flake.nix` to add `rdkafka` and `process-compose` to devShell
  - [x] Create `process-compose.yaml` for local Redpanda
  - [x] Create `Justfile` with service, kafka, and build recipes
  - [x] Added `optional-packages` for local streamly-core and streamly source trees
  - [x] Verify `cabal build all` compiles with no errors
- [x] Milestone 2: Consumer Source module (`Kafka.Streamly.Source`) (2026-04-04)
  - [x] Implement `kafkaSourceNoClose` using `Stream.unfoldrM`
  - [x] Implement `kafkaSourceAutoClose` using `Stream.bracketIO`
  - [x] Implement `kafkaSource` (fully managed) using `Stream.bracketIO`
  - [x] Implement error predicates: `isFatal`, `isPollTimeout`, `isPartitionEOF`
  - [x] Implement error filters: `skipNonFatal`, `skipNonFatalExcept`
  - [x] Implement value mapping utilities (12 functions)
  - [x] Verify module compiles
- [x] Milestone 3: Producer Sink module (`Kafka.Streamly.Sink`) (2026-04-04)
  - [x] Implement `kafkaSink` as a `Fold` that sends each `ProducerRecord`
  - [x] Implement `kafkaBatchSink` as a `Fold` that sends `[ProducerRecord]`
  - [x] Implement `withKafkaProducer` convenience bracket
  - [x] Verify module compiles
- [x] Milestone 4: Combinators module (`Kafka.Streamly.Combinators`) (2026-04-04)
  - [x] Implement `batchByOrFlush` for batching stream elements
  - [x] Implement `batchByOrFlushEither` for batching with Left-signal flush
  - [x] Implement `throwLeft` to convert error-as-value to exception
  - [x] Implement `throwLeftSatisfy` for predicate-based error throwing
  - [x] Verify module compiles
  - [x] Verify `cabal build all` succeeds for both packages
- [x] Milestone 5: Jitsurei cookbook examples (2026-04-04)
  - [x] Implement `streamly-producer` example
  - [x] Implement `streamly-consumer` example
  - [x] Implement `error-handling` example
  - [x] Implement `transform-pipeline` example
  - [x] Implement `batch-sink` example
  - [x] Implement `consume-produce` ETL example
  - [x] Implement `concurrent-consume` example using `parMapM`
  - [x] Verify all executables compile with zero warnings
  - [ ] Validate examples against running Redpanda (manual test)


## Surprises & Discoveries

- streamly-core 0.4.0 and streamly 0.12.0 are not yet on Hackage. The cabal.project uses `optional-packages` to reference the local source trees at `/Users/shinzui/Keikaku/hub/haskell/streamly-project/`. This is a temporary measure until the packages are published.

- Streamly's `Stream` type does not export a `map` function. Instead, `Stream m` has a `Functor` instance, so `fmap` is used for pure mapping. This differs from conduit's `L.map` pattern.

- `produceMessageBatch` is not exported from hw-kafka-client 5.3.0 (same discovery as kafka-effectful). The `kafkaBatchSink` implementation sends records individually in a loop instead.

- Streamly's `scanl` takes a `Scanl` type (created with `Scanl.mkScanl`), not a step function and initial value directly. The batching combinators use `Scanl.mkScanl step initial` to create the scan.

- The batching combinators required careful type design. The scan state is a triple `(Int, [a], Maybe [a])` where the third element is the batch output. After scanning, `fmap extract` extracts the `Maybe [a]` and `catMaybes` filters empties. A trailing `Stream.append ... (Stream.fromPure Nothing)` ensures the final partial batch is flushed.

- GHC2024 does not bring `Bifunctor` or `Bitraversable` into scope from Prelude. They must be explicitly imported from `Data.Bifunctor` and `Data.Bitraversable`.


## Decision Log

- Decision: Use `Stream.unfoldrM` as the core polling primitive rather than `Stream.repeatM` with `Stream.takeWhile`.
  Rationale: `unfoldrM` threads state naturally, allowing us to track whether a fatal error has occurred and stop the stream. `repeatM` produces an infinite stream with no concept of termination, which means we would need `takeWhile` or `scanMaybe` to stop on fatal errors, adding an extra combinator. `unfoldrM` is also what the Streamly docs recommend as "the most general way of generating a stream efficiently."
  Date: 2026-04-03

- Decision: Model producer sinks as `Fold m ProducerRecord (Maybe KafkaError)` rather than as stream transformations (`Stream m ProducerRecord -> Stream m (Maybe KafkaError)`).
  Rationale: A Fold is the natural dual of a Stream in Streamly. Conduit's `Sink` type is a consumer that returns a final value; Streamly's `Fold` serves the same role. Using a Fold composes cleanly with `Stream.fold` and allows the user to combine producer folds with other folds via `teeWith`, `partition`, etc. Stream transformations would require the user to drain the output stream separately, which is less ergonomic.
  Date: 2026-04-03

- Decision: Implement the three-tier resource management pattern (fully-managed, auto-close, no-close) for consumer sources but provide only no-close folds for producer sinks.
  Rationale: Consumer sources have a natural lifecycle tied to the stream: poll until done. Streamly's `bracketIO` fits perfectly. Producer folds, however, are run by `Stream.fold` which already returns to the caller; the caller can bracket the fold call in regular `Control.Exception.bracket`. Embedding resource management inside a Fold is awkward because Fold initialization happens lazily. Providing a convenience function `withKafkaProducer` that wraps bracket around the fold gives users the managed experience without fighting the Fold abstraction.
  Date: 2026-04-03

- Decision: Target `streamly-core` (0.4.0) as the primary dependency, not the `streamly` package.
  Rationale: `streamly-core` provides `Stream`, `Fold`, `Unfold`, `bracketIO`, `unfoldrM`, and all the serial stream operations we need. The `streamly` package adds concurrency (`parMapM`, `parConcatMap`, etc.) which is needed only by the jitsurei examples, not by the core library. Keeping the library dependency on `streamly-core` makes it lighter. The jitsurei package depends on `streamly` for concurrency examples.
  Date: 2026-04-03

- Decision: Structure as a multi-package project with `cabal.project` at the root, following the hw-kafka-client-project pattern.
  Rationale: The user explicitly requested a multi-package layout with a jitsurei (corpus) package. The hw-kafka-client-project provides a proven pattern: cabal.project listing packages, shared GHC version, Justfile for services, process-compose for Redpanda.
  Date: 2026-04-03

- Decision: Use `base >= 4.21` (GHC 9.12+) and `GHC2024` as the default language.
  Rationale: User explicitly requested GHC 9.12+ targeting, consistent with all their other projects (kafka-effectful, seihou, etc.).
  Date: 2026-04-03


## Outcomes & Retrospective

All five milestones completed on 2026-04-04. The library and jitsurei packages compile with zero warnings under GHC 9.12.2.

The hw-kafka-streamly library provides 3 modules with a clean API surface: `Kafka.Streamly.Source` (3 source variants, 3 error predicates, 2 error filters, 12 value mapping functions), `Kafka.Streamly.Sink` (2 fold-based sinks, 1 bracket convenience), and `Kafka.Streamly.Combinators` (2 batching combinators, 2 error-throwing combinators, 1 type).

The jitsurei package provides 7 runnable cookbook examples plus a help message, exercising every public API surface: basic production, basic consumption, three error handling patterns, three transformation patterns, batched production, ETL consume-transform-produce, and concurrent processing with `parMapM`.

Key architectural differences from hw-kafka-conduit: producer sinks are `Fold` values (not `ConduitT` sinks), resource management on the producer side uses `withKafkaProducer` (not embedded in the fold), and stream transformations are plain functions (`Stream m a -> Stream m b`) rather than conduit pipeline stages (`ConduitT a b m ()`).

Remaining work: manual end-to-end validation against a running Redpanda cluster. The examples are structurally complete but have not been tested against a live broker.


## Context and Orientation

This section describes the state of the repository and all relevant dependencies as of 2026-04-03.

### Current Repository State

The project at `/Users/shinzui/Keikaku/bokuno/hw-kafka-streamly` was scaffolded by the seihou tool and contains only build infrastructure: a `flake.nix` targeting GHC 9.12, a `treefmt.nix` for code formatting (fourmolu, nixpkgs-fmt, cabal-fmt), pre-commit hooks, and the exec-plan skill. There are no cabal files, no Haskell source files, and no `cabal.project`. The `flake.nix` does not yet include `rdkafka` (the C library that hw-kafka-client binds to) in its dev shell.

### hw-kafka-client (version 5.3.0)

hw-kafka-client is a Haskell binding to Apache Kafka via librdkafka. Its source lives at `/Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project/hw-kafka-client/`. The library exposes consumer and producer APIs through opaque handle types.

A consumer is created with `newConsumer :: MonadIO m => ConsumerProperties -> Subscription -> m (Either KafkaError KafkaConsumer)` and destroyed with `closeConsumer :: MonadIO m => KafkaConsumer -> m (Maybe KafkaError)`. Messages are retrieved by calling `pollMessage :: MonadIO m => KafkaConsumer -> Timeout -> m (Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))`. Each poll either returns a message (`Right record`) or an error (`Left err`). Non-fatal errors include poll timeouts (`RdKafkaRespErrTimedOut`) and partition EOF (`RdKafkaRespErrPartitionEof`). Fatal errors include authentication failures, SSL errors, and invalid configuration.

A producer is created with `newProducer :: MonadIO m => ProducerProperties -> m (Either KafkaError KafkaProducer)` and destroyed with `closeProducer :: MonadIO m => KafkaProducer -> m ()`. Messages are sent with `produceMessage :: MonadIO m => KafkaProducer -> ProducerRecord -> m (Maybe KafkaError)` where `Nothing` means success and `Just err` means failure. Batches can be sent with `produceMessageBatch :: MonadIO m => KafkaProducer -> [ProducerRecord] -> m [(ProducerRecord, KafkaError)]` which returns only the failed records.

Configuration uses a Monoid builder pattern: `brokersList ["localhost:9092"] <> groupId (ConsumerGroupId "my-group") <> noAutoCommit`.

Key types: `ConsumerRecord k v` has fields `crTopic`, `crPartition`, `crOffset`, `crTimestamp`, `crHeaders`, `crKey`, `crValue`. It has `Functor`, `Bifunctor`, `Foldable`, `Traversable`, `Bifoldable`, and `Bitraversable` instances. `ProducerRecord` has fields `prTopic`, `prPartition`, `prKey`, `prValue`, `prHeaders`. `KafkaError` is an ADT with constructors like `KafkaResponseError RdKafkaRespErrT`, `KafkaBadSpecification Text`, etc. It has an `Exception` instance.

### hw-kafka-conduit (version 2.7.0)

hw-kafka-conduit is the existing streaming integration for hw-kafka-client using the conduit library. Its source lives at `/Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project/hw-kafka-conduit/`. It has three modules:

`Kafka.Conduit.Source` provides three consumer source variants with different resource management tiers. `kafkaSource` creates a consumer, polls indefinitely yielding `Either KafkaError (ConsumerRecord ...)`, and closes the consumer when the pipeline ends. It uses conduit's `bracketP` for resource safety. `kafkaSourceAutoClose` takes an existing consumer and closes it on completion. `kafkaSourceNoClose` takes an existing consumer and does not close it. The module also exports error predicates (`isFatal` recognizes 14 specific fatal error types, `isPollTimeout`, `isPartitionEOF`), error filters (`skipNonFatal` drops non-fatal errors, `skipNonFatalExcept` takes a list of predicates for non-fatal errors to keep), and value-mapping utilities that work on `ConsumerRecord` inside `Either KafkaError` (`mapValue`, `mapFirst`, `bimapValue`, `sequenceValue`, `sequenceValueFirst`, `bisequenceValue`, `traverseValue`, `traverseValueFirst`, `bitraverseValue`, `traverseValueM`, `traverseValueFirstM`, `bitraverseValueM`).

`Kafka.Conduit.Sink` provides producer sink variants. `kafkaSink` creates a producer, consumes `ProducerRecord` inputs, sends each to Kafka, and closes the producer on completion. Returns `Maybe KafkaError` (Nothing on success, Just on first error). `kafkaSinkAutoClose` takes an existing producer and closes it. `kafkaSinkNoClose` takes an existing producer without closing. `kafkaBatchSinkNoClose` takes `[ProducerRecord]` inputs.

`Kafka.Conduit.Combinators` provides `batchByOrFlush` (batches elements by count, flushing on `Nothing` signal), `batchByOrFlushEither` (batches, flushing on `Left` signal), `foldYield` (stateful fold that can emit multiple outputs per input), `throwLeft` (converts `Left` to exception), and `throwLeftSatisfy` (throws `Left` values matching a predicate). `BatchSize` is a newtype around `Int`.

### Streamly (streamly-core 0.4.0, streamly 0.12.0)

Streamly is a high-performance streaming library for Haskell. Its source lives at `/Users/shinzui/Keikaku/hub/haskell/streamly-project/`. The library is split into two packages: `streamly-core` provides serial streaming, folds, unfolds, parsers, arrays, and file I/O; `streamly` adds concurrency, time-related combinators, and lifted exceptions on top of `streamly-core`.

The core streaming type is `Stream m a` which represents a producer of values in monad `m`. Streams are generated with combinators like `unfoldrM :: Monad m => (s -> m (Maybe (a, s))) -> s -> Stream m a` (the most general generator, threading state through each step, returning `Nothing` to terminate), `repeatM :: Monad m => m a -> Stream m a` (infinite stream from a repeated action), and `fromList :: Applicative m => [a] -> Stream m a`.

Streams are consumed with folds: `Stream.fold :: Monad m => Fold m a b -> Stream m a -> m b`. The `Fold m a b` type represents a consumer that reduces a stream to a value. Folds support early termination via the `Done` step constructor. Key fold combinators include `Fold.foldlM' :: Monad m => (b -> a -> m b) -> m b -> Fold m a b` for strict monadic left folds, `Fold.drain :: Monad m => Fold m a ()` to discard all elements, `Fold.drainMapM :: Monad m => (a -> m b) -> Fold m a ()` to apply a monadic action and discard results, and `Fold.toList :: Monad m => Fold m a [a]`.

Stream transformations use `Stream.mapM :: Monad m => (a -> m b) -> Stream m a -> Stream m b`, `Stream.filter :: Monad m => (a -> Bool) -> Stream m a -> Stream m a`, `Stream.take :: Monad m => Int -> Stream m a -> Stream m a`, and `Stream.scanl :: Monad m => Scanl m a b -> Stream m a -> Stream m b`.

Resource management uses `Stream.bracketIO :: (MonadIO m, MonadCatch m) => IO b -> (b -> IO c) -> (b -> Stream m a) -> Stream m a` which acquires a resource, uses it to create a stream, and releases the resource when the stream completes, errors, or is garbage collected. For guaranteed cleanup regardless of how the stream is consumed, `Stream.bracketIO' :: MonadIO m => AcquireIO -> IO b -> (b -> IO c) -> (b -> Stream m a) -> Stream m a` works with an `AcquireIO` scope created by `withAcquireIO`.

Concurrency (from the `streamly` package, not `streamly-core`) provides `parMapM :: MonadAsync m => (Config -> Config) -> (a -> m b) -> Stream m a -> Stream m b` for parallel mapping, `parConcatMap :: MonadAsync m => (Config -> Config) -> (a -> Stream m b) -> Stream m a -> Stream m b` for parallel flat-mapping, `parList :: MonadAsync m => (Config -> Config) -> [Stream m a] -> Stream m a` for merging multiple streams concurrently, and `parBuffered :: MonadAsync m => (Config -> Config) -> Stream m a -> Stream m a` for decoupling producer and consumer via buffering. Configuration includes `maxThreads`, `maxBuffer`, `maxRate`, `ordered`, `eager`, and `interleaved`.

### kafka-effectful

The user's own library at `/Users/shinzui/Keikaku/bokuno/libraries/haskell/kafka-effectful` wraps hw-kafka-client with effectful effects. It serves as a style reference for this project. Key patterns: GHC2024 default language, `base >= 4.21`, consistent GHC warning flags (`-Wall -Wcompat -Widentities -Wincomplete-uni-patterns -Wincomplete-record-updates -Wredundant-constraints -fhide-source-paths -Wmissing-export-lists -Wpartial-fields -Wmissing-deriving-strategies`), default extensions including `DataKinds`, `DuplicateRecordFields`, `ImportQualifiedPost`, `LambdaCase`, `NoFieldSelectors`, `OverloadedRecordDot`, `OverloadedStrings`, `TypeFamilies`, `TypeOperators`. The cabal-version is 3.4.

### hw-kafka-client-project (multi-package reference)

The project at `/Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project` demonstrates the exact multi-package pattern we will follow. Its `cabal.project` lists four packages: `hw-kafka-client`, `hw-kafka-conduit`, `hw-kafka-client-jitsurei`, and `hw-kafka-conduit-jitsurei`. It uses `with-compiler: ghc-9.12.2` and has `allow-newer` stanzas for GHC 9.12 boot library compatibility. The `flake.nix` includes `rdkafka`, `zlib`, `just`, `cabal-install`, `pkg-config`, and HLS in its dev shell, with `process-compose` enabled. The `process-compose.yaml` runs Redpanda via `rpk container start -n 1 --kafka-ports 9092` with a readiness probe on `rpk cluster info`. The `Justfile` has groups for services (process-up/down), kafka (create-topic/delete-topic/list-topics), and build (build/clean/fmt). The `treefmt.nix` excludes subtree directories from formatting.

The jitsurei packages follow a consistent structure: a library stanza exposing a single `Config` module with shared defaults (broker address, timeout, topic name), plus multiple executable stanzas each in `app/` with a single Main module. The `hw-kafka-conduit-jitsurei` has 7 executables demonstrating conduit streaming patterns: basic producer, basic consumer, error handling (three strategies), transform pipeline, batch sink, consume-produce ETL, and manual resource management.


## Plan of Work

The work proceeds in five milestones. Each milestone produces a compilable codebase and can be validated independently.


### Milestone 1: Project scaffolding and build infrastructure

This milestone creates the multi-package project structure, both cabal files, placeholder Haskell modules, and all build infrastructure. At the end, `cabal build all` succeeds with no errors against GHC 9.12.

Create `cabal.project` in the repository root listing both packages with GHC 9.12 configuration and allow-newer stanzas for boot library compatibility.

Create `hw-kafka-streamly/hw-kafka-streamly.cabal` as a library package. The library depends on `base >= 4.21 && < 5`, `hw-kafka-client >= 5.3 && < 6`, `streamly-core >= 0.4 && < 0.5`, `bytestring >= 0.11 && < 0.13`, and `bifunctors >= 5.6 && < 6`. It exposes three modules: `Kafka.Streamly.Source`, `Kafka.Streamly.Sink`, and `Kafka.Streamly.Combinators`. Use cabal-version 3.4, GHC2024 default language, and the same warning flags and default extensions as kafka-effectful. Create a `hw-kafka-streamly` directory for this package.

Create placeholder source files for each exposed module with just the module declaration and an empty export list. Place them under `hw-kafka-streamly/src/Kafka/Streamly/`.

Create `hw-kafka-streamly-jitsurei/hw-kafka-streamly-jitsurei.cabal` as a library plus executable package. The library exposes `HwKafkaStreamly.Jitsurei.Config` with shared defaults (broker address, timeout, topic name). Define executable stanzas for: `hw-kafka-streamly-jitsurei` (help), `streamly-producer`, `streamly-consumer`, `error-handling`, `transform-pipeline`, `batch-sink`, `consume-produce`, and `concurrent-consume`. Each executable lives in `app/` with `-threaded -rtsopts -with-rtsopts=-N`. The executables depend on `hw-kafka-streamly`, `hw-kafka-streamly-jitsurei`, `hw-kafka-client`, `streamly-core`, `bytestring`, and the concurrency example also depends on `streamly`. Create a `hw-kafka-streamly-jitsurei` directory for this package.

Create the jitsurei Config module at `hw-kafka-streamly-jitsurei/src/HwKafkaStreamly/Jitsurei/Config.hs` exporting `defaultBrokerAddress`, `defaultTimeout`, `defaultTopicName`, and `outputTopicName`. Create `hw-kafka-streamly-jitsurei/app/Main.hs` as a help message listing available examples. Create placeholder Main modules for each executable that print "TODO".

Update `flake.nix`: add `pkgs.rdkafka` to `nativeBuildInputs`, set `withProcessCompose = true`, change the default package to `haskellPackages.hw-kafka-streamly-jitsurei`.

Create `process-compose.yaml` identical to the hw-kafka-client-project version (Redpanda via rpk).

Create `Justfile` with groups: services (process-up, process-down), kafka (create-topic, delete-topic, list-topics), and build (build, clean, fmt).

Acceptance: running `cabal build all` from the repository root produces no errors.

    cd /Users/shinzui/Keikaku/bokuno/hw-kafka-streamly
    cabal build all

Expected: compilation succeeds for both `hw-kafka-streamly` and `hw-kafka-streamly-jitsurei` (including all executables).


### Milestone 2: Consumer Source module

This milestone implements `Kafka.Streamly.Source`, the module that turns a Kafka consumer into a Streamly stream. At the end, the module compiles and exports all source variants, error predicates, error filters, and value mapping utilities.

In `hw-kafka-streamly/src/Kafka/Streamly/Source.hs`, implement the following public API.

The core polling function is `kafkaSourceNoClose`. It takes a `KafkaConsumer` and a `Timeout` and returns `Stream m (Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))`. It uses `Stream.unfoldrM` with a `Bool` state tracking whether the stream should continue. Each step calls `pollMessage consumer timeout` via `liftIO`. If the result is a fatal error, yield it and set the state to `False` (terminating the stream on the next step). Otherwise yield the result and continue.

    kafkaSourceNoClose :: MonadIO m
                       => KafkaConsumer
                       -> Timeout
                       -> Stream m (Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))

`kafkaSourceAutoClose` wraps `kafkaSourceNoClose` with `Stream.bracketIO`. The acquire action returns the consumer as-is (`pure consumer`). The release action calls `closeConsumer` and discards the result.

    kafkaSourceAutoClose :: (MonadIO m, MonadCatch m)
                         => KafkaConsumer
                         -> Timeout
                         -> Stream m (Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))

`kafkaSource` is fully managed. The acquire action calls `newConsumer props sub` and throws the `KafkaError` as an exception (via `throwIO`) if it fails, otherwise returns the consumer. The release action calls `closeConsumer`. The stream action delegates to `kafkaSourceNoClose`.

    kafkaSource :: (MonadIO m, MonadCatch m)
                => ConsumerProperties
                -> Subscription
                -> Timeout
                -> Stream m (Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))

Error predicates: `isFatal` returns `True` for exactly the same 14 error types as hw-kafka-conduit (UnknownConfigurationKey, InvalidConfigurationValue, BadConfiguration, BadSpecification, and response errors Destroy, Fail, InvalidArg, Ssl, UnknownProtocol, NotImplemented, Authentication, InconsistentGroupProtocol, TopicAuthorizationFailed, GroupAuthorizationFailed, ClusterAuthorizationFailed, UnsupportedSaslMechanism, IllegalSaslState, UnsupportedVersion). `isPollTimeout` checks for `KafkaResponseError RdKafkaRespErrTimedOut`. `isPartitionEOF` checks for `KafkaResponseError RdKafkaRespErrPartitionEof`.

Error filters: `skipNonFatal` uses `Stream.filter` to drop `Left err` values where `not (isFatal err)`, passing through all `Right` values and fatal `Left` values. `skipNonFatalExcept` takes a list of predicates `[KafkaError -> Bool]` and keeps `Left err` values where `isFatal err` or any predicate returns `True`.

Value mapping utilities operate on `Either KafkaError (ConsumerRecord k v)` inside a stream. They mirror hw-kafka-conduit's API exactly: `mapValue` maps the value field, `mapFirst` maps the key field, `bimapValue` maps both. The `sequence*` variants lift an `Applicative` out. The `traverse*` variants apply a pure function. The `traverse*M` variants apply a monadic function. These are implemented as `Stream.map` or `Stream.mapM` with appropriate `fmap`/`bimap`/`traverse`/`bitraverse` on the `ConsumerRecord`. The `ConsumerRecord` type already has `Functor`, `Bifunctor`, `Foldable`, `Traversable`, `Bifoldable`, and `Bitraversable` instances from hw-kafka-client.

Acceptance: `cabal build hw-kafka-streamly` succeeds.


### Milestone 3: Producer Sink module

This milestone implements `Kafka.Streamly.Sink`, providing Streamly Folds that send records to a Kafka producer. At the end, the module compiles and exports all sink functions.

In `hw-kafka-streamly/src/Kafka/Streamly/Sink.hs`, implement the following.

`kafkaSink` is a Fold that sends each `ProducerRecord` to Kafka via the given `KafkaProducer`. It accumulates `Maybe KafkaError`: starts as `Nothing`, calls `produceMessage producer record` for each element. On success (`Nothing` from `produceMessage`), continues. On failure (`Just err`), terminates the fold immediately. Implementation uses `Fold.foldlM'` with early-exit via the accumulator pattern: once the state is `Just err`, subsequent steps are no-ops.

    kafkaSink :: MonadIO m
              => KafkaProducer
              -> Fold m ProducerRecord (Maybe KafkaError)

`kafkaBatchSink` accepts `[ProducerRecord]` inputs and uses `produceMessageBatch producer batch` to send them. It returns on the first batch that has any failures.

    kafkaBatchSink :: MonadIO m
                   => KafkaProducer
                   -> Fold m [ProducerRecord] (Maybe KafkaError)

`withKafkaProducer` is a convenience function that brackets producer creation and destruction around a user-provided action. It calls `newProducer`, passes the producer to the action, flushes and closes the producer on completion.

    withKafkaProducer :: ProducerProperties
                      -> (KafkaProducer -> IO a)
                      -> IO (Either KafkaError a)

Acceptance: `cabal build hw-kafka-streamly` succeeds.


### Milestone 4: Combinators module

This milestone implements `Kafka.Streamly.Combinators`, providing batching and error-handling combinators for Streamly streams. At the end, `cabal build all` succeeds for both packages.

In `hw-kafka-streamly/src/Kafka/Streamly/Combinators.hs`, implement the following.

`BatchSize` is a newtype around `Int` with `Show` and `Eq` instances.

`batchByOrFlush` takes a `BatchSize` and transforms a `Stream m (Maybe a)` into a `Stream m [a]`. Elements wrapped in `Just` accumulate into a batch. When the batch reaches the specified size or a `Nothing` is received, the current batch is emitted as a list. Empty batches (consecutive `Nothing` signals) are not emitted. Implementation uses `Stream.scanl` with a `Fold` that accumulates a list and terminates on batch-full or Nothing.

    batchByOrFlush :: Monad m => BatchSize -> Stream m (Maybe a) -> Stream m [a]

`batchByOrFlushEither` is similar but uses `Left` values as flush signals and `Right` values as batch elements.

    batchByOrFlushEither :: Monad m => BatchSize -> Stream m (Either e a) -> Stream m [a]

`throwLeft` converts a stream of `Either e a` where `e` has an `Exception` instance into a stream of `a`, throwing any `Left` values as exceptions.

    throwLeft :: (MonadIO m, Exception e) => Stream m (Either e a) -> Stream m a

`throwLeftSatisfy` throws only `Left` values where the predicate returns `True`, dropping other `Left` values silently and passing `Right` values through.

    throwLeftSatisfy :: (MonadIO m, Exception e) => (e -> Bool) -> Stream m (Either e a) -> Stream m a

Acceptance: `cabal build all` succeeds for both packages.


### Milestone 5: Jitsurei cookbook examples

This milestone implements all jitsurei executables, each demonstrating a specific pattern. At the end, all executables compile and can be run against a local Redpanda cluster.

All examples import `HwKafkaStreamly.Jitsurei.Config` for shared defaults: `defaultBrokerAddress` is `"localhost:9092"`, `defaultTimeout` is `Timeout 10000`, `defaultTopicName` is `TopicName "jitsurei-topic"`, `outputTopicName` is `TopicName "jitsurei-streamly-output"`.

**streamly-producer** (`app/StreamlyProducer.hs`): Creates a producer with `newProducer`, builds a stream of 100 `ProducerRecord` values using `Stream.fromList`, folds the stream with `kafkaSink`, flushes and closes the producer. Prints each sent message and the final result.

**streamly-consumer** (`app/StreamlyConsumer.hs`): Uses `kafkaSource` with consumer properties (group id "jitsurei-streamly-group", no auto commit, offset reset Earliest), subscribes to the topic, takes 10 messages with `Stream.take 10`, and folds with `Fold.toList`. Prints each received message.

**error-handling** (`app/ErrorHandling.hs`): Demonstrates three error handling strategies from hw-kafka-conduit ported to Streamly:
1. `skipNonFatal` to filter timeouts and EOF
2. `skipNonFatalExcept [isPollTimeout]` to keep timeout visibility
3. `throwLeft` after `skipNonFatal` to convert remaining fatal errors to exceptions

**transform-pipeline** (`app/TransformPipeline.hs`): Demonstrates `mapValue` (uppercase), `mapFirst` (key prefix), and `bimapValue` (both), consuming from Kafka and printing transformed records.

**batch-sink** (`app/BatchSink.hs`): Consumes from Kafka, collects into batches using `batchByOrFlush`, and produces each batch to the output topic via `kafkaBatchSink`.

**consume-produce** (`app/ConsumeProduce.hs`): ETL pipeline. Consumes from one topic, transforms messages (e.g., adds a header, modifies value), produces to another topic. Uses `kafkaSource` for input, `Stream.mapM` for the produce step, and `Stream.fold Fold.drain` to drive the pipeline.

**concurrent-consume** (`app/ConcurrentConsume.hs`): Demonstrates concurrent message processing using `parMapM` from the `streamly` package. Consumes messages and processes each with simulated latency, showing how Streamly's concurrency enables parallel processing with configurable thread count and buffer size.

**hw-kafka-streamly-jitsurei** (`app/Main.hs`): Prints a help message listing all available example executables with brief descriptions.

Acceptance: all executables compile via `cabal build all`. Manual testing: start Redpanda with `just process-up`, create topic with `just create-topic`, run `cabal run streamly-producer` then `cabal run streamly-consumer` and observe messages flowing.

    cd /Users/shinzui/Keikaku/bokuno/hw-kafka-streamly
    just process-up
    just create-topic
    cabal run streamly-producer
    cabal run streamly-consumer

Expected: the producer prints 100 "sent" messages, the consumer prints 10 received messages with keys and values.


## Concrete Steps

All commands assume the working directory is `/Users/shinzui/Keikaku/bokuno/hw-kafka-streamly` unless stated otherwise.

### Milestone 1 commands

Create the directory structure:

    mkdir -p hw-kafka-streamly/src/Kafka/Streamly
    mkdir -p hw-kafka-streamly-jitsurei/src/HwKafkaStreamly/Jitsurei
    mkdir -p hw-kafka-streamly-jitsurei/app

Write all configuration files (cabal.project, both .cabal files, flake.nix updates, Justfile, process-compose.yaml) and placeholder Haskell modules.

Verify the build:

    cabal build all

Expected output should end with a line like:

    Building library for hw-kafka-streamly-0.1.0.0...
    Building library for hw-kafka-streamly-jitsurei-0.1.0.0...

No errors.

### Milestone 2 commands

Write `hw-kafka-streamly/src/Kafka/Streamly/Source.hs` with the full implementation.

    cabal build hw-kafka-streamly

Expected: compiles with at most warnings (no errors).

### Milestone 3 commands

Write `hw-kafka-streamly/src/Kafka/Streamly/Sink.hs` with the full implementation.

    cabal build hw-kafka-streamly

Expected: compiles successfully.

### Milestone 4 commands

Write `hw-kafka-streamly/src/Kafka/Streamly/Combinators.hs` with the full implementation.

    cabal build all

Expected: both packages compile successfully.

### Milestone 5 commands

Write all example executables under `hw-kafka-streamly-jitsurei/app/`.

    cabal build all

Expected: all executables compile. Then for manual validation:

    just process-up
    # Wait for readiness probe
    just create-topic
    cabal run streamly-producer
    cabal run streamly-consumer
    just process-down


## Validation and Acceptance

The system is complete when:

1. `cabal build all` compiles both packages and all executables with no errors.

2. The `streamly-producer` executable sends 100 messages to `jitsurei-topic` on a local Redpanda and prints confirmation for each.

3. The `streamly-consumer` executable consumes messages from `jitsurei-topic` and prints 10 messages before stopping.

4. The `consume-produce` executable reads from `jitsurei-topic`, transforms, and writes to `jitsurei-streamly-output`.

5. The `concurrent-consume` executable processes messages concurrently, demonstrating parallelism.

6. The `error-handling` executable demonstrates all three error strategies without crashing on non-fatal errors.

7. Every public module has an explicit export list (enforced by `-Wmissing-export-lists`).

8. All code passes `treefmt` formatting checks.


## Idempotence and Recovery

All steps are file-creation or file-editing operations that can be re-run safely. If a step fails partway through, re-running it from the beginning overwrites incomplete files with correct contents.

The `cabal build all` command is always safe to re-run. If dependencies change, `cabal clean` followed by `cabal build all` provides a fresh build.

The Redpanda process-compose setup is idempotent: `rpk container start` is a no-op if already running, and `rpk container purge` cleans up fully. Topics can be re-created after deletion with `just create-topic`.


## Interfaces and Dependencies

### Library: hw-kafka-streamly

Dependencies:
- `base >= 4.21 && < 5` (GHC 9.12+ standard library)
- `hw-kafka-client >= 5.3 && < 6` (Kafka bindings via librdkafka)
- `streamly-core >= 0.4 && < 0.5` (serial streams, folds, resource management)
- `bytestring >= 0.11 && < 0.13` (for `ByteString` in record types)
- `bifunctors >= 5.6 && < 6` (for `Bifunctor`, `Bitraversable` operations on `ConsumerRecord`)

Exposed modules and their key functions:

In `hw-kafka-streamly/src/Kafka/Streamly/Source.hs`:

    -- Stream sources
    kafkaSource :: (MonadIO m, MonadCatch m)
                => ConsumerProperties -> Subscription -> Timeout
                -> Stream m (Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))

    kafkaSourceAutoClose :: (MonadIO m, MonadCatch m)
                         => KafkaConsumer -> Timeout
                         -> Stream m (Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))

    kafkaSourceNoClose :: MonadIO m
                       => KafkaConsumer -> Timeout
                       -> Stream m (Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))

    -- Error predicates
    isFatal :: KafkaError -> Bool
    isPollTimeout :: KafkaError -> Bool
    isPartitionEOF :: KafkaError -> Bool

    -- Error filters
    skipNonFatal :: Monad m
                 => Stream m (Either KafkaError a)
                 -> Stream m (Either KafkaError a)

    skipNonFatalExcept :: Monad m
                       => [KafkaError -> Bool]
                       -> Stream m (Either KafkaError a)
                       -> Stream m (Either KafkaError a)

    -- Value mapping (12 functions)
    mapValue :: Monad m => (v -> v') -> Stream m (Either e (ConsumerRecord k v)) -> Stream m (Either e (ConsumerRecord k v'))
    mapFirst :: Monad m => (k -> k') -> Stream m (Either e (ConsumerRecord k v)) -> Stream m (Either e (ConsumerRecord k' v))
    bimapValue :: Monad m => (k -> k') -> (v -> v') -> Stream m (Either e (ConsumerRecord k v)) -> Stream m (Either e (ConsumerRecord k' v'))
    -- ... and 9 more (sequence*, traverse*, traverse*M variants)

In `hw-kafka-streamly/src/Kafka/Streamly/Sink.hs`:

    kafkaSink :: MonadIO m => KafkaProducer -> Fold m ProducerRecord (Maybe KafkaError)

    kafkaBatchSink :: MonadIO m => KafkaProducer -> Fold m [ProducerRecord] (Maybe KafkaError)

    withKafkaProducer :: ProducerProperties -> (KafkaProducer -> IO a) -> IO (Either KafkaError a)

In `hw-kafka-streamly/src/Kafka/Streamly/Combinators.hs`:

    newtype BatchSize = BatchSize { unBatchSize :: Int }

    batchByOrFlush :: Monad m => BatchSize -> Stream m (Maybe a) -> Stream m [a]

    batchByOrFlushEither :: Monad m => BatchSize -> Stream m (Either e a) -> Stream m [a]

    throwLeft :: (MonadIO m, Exception e) => Stream m (Either e a) -> Stream m a

    throwLeftSatisfy :: (MonadIO m, Exception e) => (e -> Bool) -> Stream m (Either e a) -> Stream m a

### Jitsurei: hw-kafka-streamly-jitsurei

Dependencies:
- `base >= 4.21 && < 5`
- `hw-kafka-streamly` (the library above)
- `hw-kafka-client >= 5.3 && < 6` (for types used in examples)
- `streamly-core >= 0.4 && < 0.5` (for stream operations in examples)
- `streamly >= 0.12 && < 0.13` (only for `concurrent-consume`, for `parMapM`)
- `bytestring >= 0.11 && < 0.13`

Exposed module:

In `hw-kafka-streamly-jitsurei/src/HwKafkaStreamly/Jitsurei/Config.hs`:

    defaultBrokerAddress :: BrokerAddress
    defaultTimeout :: Timeout
    defaultTopicName :: TopicName
    outputTopicName :: TopicName
