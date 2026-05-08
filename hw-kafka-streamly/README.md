# hw-kafka-streamly

Streamly streaming integration for [`hw-kafka-client`][hw-kafka-client] — the
Haskell binding to Apache Kafka via [librdkafka][librdkafka].

`hw-kafka-streamly` exposes Kafka consumers as composable Streamly `Stream`s
and Kafka producers as Streamly `Fold`s, so message processing can be expressed
in the same vocabulary used elsewhere in a Streamly pipeline. Resource handling
(consumer/producer creation, close, flush) is layered so the caller picks the
amount of bracketing they want.

## Installation

Add to your `.cabal` file:

```cabal
build-depends:
  , hw-kafka-streamly  >=0.2 && <0.3
  , hw-kafka-client    >=5.3 && <6
  , streamly-core      >=0.3 && <0.5
```

`hw-kafka-client` needs the `librdkafka` C library at build and run time
(Homebrew: `brew install librdkafka`; Debian/Ubuntu: `apt install
librdkafka-dev`).

## Modules

- `Kafka.Streamly.Stream` — consumer streams, error predicates and filters,
  value-mapping helpers built on `Bifunctor`/`Bitraversable`.
- `Kafka.Streamly.Fold` — producer folds and a `withKafkaProducer` bracket
  helper.
- `Kafka.Streamly.Combinators` — batching combinators and helpers that throw
  `Left` values as exceptions.

## Consuming

The stream module ships three variants that differ only in how they manage
the underlying `KafkaConsumer`:

- `kafkaStream` — creates the consumer from `ConsumerProperties` and
  `Subscription`, closes it when the stream ends. Use this when the stream
  fully owns the consumer's lifecycle.
- `kafkaStreamAutoClose` — wraps a caller-supplied `KafkaConsumer` and closes
  it on stream end. Use this when the consumer is created elsewhere but its
  lifetime matches the stream.
- `kafkaStreamNoClose` — wraps a caller-supplied `KafkaConsumer` and leaves
  it open. Use this when the consumer outlives the stream.

```haskell
import Kafka.Consumer
import Kafka.Streamly.Stream (kafkaStream, skipNonFatal)
import Streamly.Data.Stream qualified as Stream

main :: IO ()
main = do
  let props = brokersList ["localhost:9092"]
           <> groupId "my-group"
           <> noAutoCommit
      sub  = topics ["events"] <> offsetReset Earliest
  Stream.fold (Fold.drainBy print)
    . skipNonFatal
    $ kafkaStream props sub (Timeout 1000)
```

## Producing

Producer folds are Streamly `Fold`s:

```haskell
import Kafka.Producer
import Kafka.Streamly.Fold (kafkaFold, withKafkaProducer)
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream qualified as Stream

main :: IO ()
main = do
  let props = brokersList ["localhost:9092"]
  result <- withKafkaProducer props $ \producer ->
    Stream.fold (kafkaFold producer)
      . fmap mkRecord
      $ Stream.fromList ["a", "b", "c"]
  print result
  where
    mkRecord v =
      ProducerRecord
        { prTopic     = TopicName "events"
        , prPartition = UnassignedPartition
        , prKey       = Nothing
        , prValue     = Just v
        , prHeaders   = mempty
        }
```

## Cookbook

For end-to-end runnable examples — concurrent consumers, batching producers,
error handling, transform pipelines — see the companion
[`hw-kafka-streamly-jitsurei`][jitsurei] package in this repository. It is not
published to Hackage; clone the repo and run the executables locally.

## Design notes

The original design plan for the bindings lives at
`docs/plans/1-streamly-bindings-for-hw-kafka-client.md`.

## License

MIT — see [LICENSE](./LICENSE).

[hw-kafka-client]: https://hackage.haskell.org/package/hw-kafka-client
[librdkafka]: https://github.com/confluentinc/librdkafka
[jitsurei]: ../hw-kafka-streamly-jitsurei
