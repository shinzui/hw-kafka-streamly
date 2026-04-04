let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/8415b4b8a746a84eecf982f0f1d7194368bf7b54/package.dhall
        sha256:d19ae156d6c357d982a1aea0f1b6ba1f01d76d2d848545b150db75ed4c39a8a9

let ContentType =
      https://raw.githubusercontent.com/shinzui/mori-schema/8415b4b8a746a84eecf982f0f1d7194368bf7b54/extensions/cookbook/ContentType.dhall

let Topic =
      https://raw.githubusercontent.com/shinzui/mori-schema/8415b4b8a746a84eecf982f0f1d7194368bf7b54/extensions/cookbook/Topic.dhall

in  { entries =
      [ { key = "streamly-consumer"
        , title = "Consume Kafka messages with kafkaSource"
        , contentType = ContentType.SampleCode
        , topics = [ Topic.Streaming ]
        , packages = [ "hw-kafka-streamly", "hw-kafka-client", "streamly-core" ]
        , language = Schema.Language.Haskell
        , audience = Schema.DocAudience.User
        , location =
            Schema.DocLocation.LocalFile
              "hw-kafka-streamly-jitsurei/app/StreamlyConsumer.hs"
        , description = Some
            "Basic Kafka consumer using kafkaSource with Stream.take and fold-based draining"
        }
      , { key = "streamly-producer"
        , title = "Produce Kafka messages with kafkaSink"
        , contentType = ContentType.SampleCode
        , topics = [ Topic.Streaming ]
        , packages = [ "hw-kafka-streamly", "hw-kafka-client", "streamly-core" ]
        , language = Schema.Language.Haskell
        , audience = Schema.DocAudience.User
        , location =
            Schema.DocLocation.LocalFile
              "hw-kafka-streamly-jitsurei/app/StreamlyProducer.hs"
        , description = Some
            "Produce messages using withKafkaProducer and kafkaSink fold"
        }
      , { key = "error-handling"
        , title = "Handle Kafka consumer errors with skipNonFatal and throwLeft"
        , contentType = ContentType.Pattern
        , topics = [ Topic.Streaming, Topic.ErrorHandling ]
        , packages = [ "hw-kafka-streamly", "hw-kafka-client", "streamly-core" ]
        , language = Schema.Language.Haskell
        , audience = Schema.DocAudience.User
        , location =
            Schema.DocLocation.LocalFile
              "hw-kafka-streamly-jitsurei/app/ErrorHandling.hs"
        , description = Some
            "Three error handling patterns: skipNonFatal, skipNonFatalExcept with timeout detection, and throwLeft with exception catching"
        }
      , { key = "transform-pipeline"
        , title = "Transform consumed Kafka records with mapValue and Bifunctor"
        , contentType = ContentType.Pattern
        , topics = [ Topic.Streaming ]
        , packages = [ "hw-kafka-streamly", "hw-kafka-client", "streamly-core" ]
        , language = Schema.Language.Haskell
        , audience = Schema.DocAudience.User
        , location =
            Schema.DocLocation.LocalFile
              "hw-kafka-streamly-jitsurei/app/TransformPipeline.hs"
        , description = Some
            "Transform record values with mapValue, keys with Bifunctor.first, and both with bimap"
        }
      , { key = "batch-sink"
        , title = "Batch-produce Kafka messages with kafkaBatchSink"
        , contentType = ContentType.SampleCode
        , topics = [ Topic.Streaming, Topic.Performance ]
        , packages = [ "hw-kafka-streamly", "hw-kafka-client", "streamly-core" ]
        , language = Schema.Language.Haskell
        , audience = Schema.DocAudience.User
        , location =
            Schema.DocLocation.LocalFile
              "hw-kafka-streamly-jitsurei/app/BatchSink.hs"
        , description = Some
            "Batch messages with batchByOrFlush and produce via kafkaBatchSink"
        }
      , { key = "consume-produce"
        , title = "Consume, transform, and produce Kafka messages in a pipeline"
        , contentType = ContentType.Pattern
        , topics = [ Topic.Streaming ]
        , packages = [ "hw-kafka-streamly", "hw-kafka-client", "streamly-core" ]
        , language = Schema.Language.Haskell
        , audience = Schema.DocAudience.User
        , location =
            Schema.DocLocation.LocalFile
              "hw-kafka-streamly-jitsurei/app/ConsumeProduce.hs"
        , description = Some
            "End-to-end pipeline: consume from one topic, transform, produce to another topic"
        }
      , { key = "concurrent-consume"
        , title = "Process Kafka messages concurrently with parMapM"
        , contentType = ContentType.Pattern
        , topics = [ Topic.Streaming, Topic.Performance ]
        , packages =
          [ "hw-kafka-streamly"
          , "hw-kafka-client"
          , "streamly-core"
          , "streamly"
          ]
        , language = Schema.Language.Haskell
        , audience = Schema.DocAudience.User
        , location =
            Schema.DocLocation.LocalFile
              "hw-kafka-streamly-jitsurei/app/ConcurrentConsume.hs"
        , description = Some
            "Concurrent message processing using Streamly parMapM with maxThreads and maxBuffer"
        }
      ]
    }
