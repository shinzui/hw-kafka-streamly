let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/02a8a876f6f7074510eb03071116d57f5529378b/package.dhall
        sha256:a19f5dd9181db28ba7a6a1b77b5ab8715e81aba3e2a8f296f40973003a0b4412

let Cookbook =
      https://raw.githubusercontent.com/shinzui/mori-schema/02a8a876f6f7074510eb03071116d57f5529378b/extensions/cookbook/package.dhall
        sha256:ebad17941153398677bb37b4f7913db1b2186664c92999e10934b94dbd6db66f

in  Cookbook.CookbookCatalog::{ entries =
      [ Cookbook.CookbookEntry::{ key = "streamly-consumer"
        , title = "Consume Kafka messages with kafkaSource"
        , contentType = Cookbook.ContentType.SampleCode
        , topics = [ Cookbook.Topic.Streaming ]
        , packages = [ "hw-kafka-streamly", "hw-kafka-client", "streamly-core" ]
        , language = Schema.Language.Haskell
        , audience = Schema.DocAudience.User
        , location =
            Schema.DocLocation.LocalFile
              "hw-kafka-streamly-jitsurei/app/StreamlyConsumer.hs"
        , description = Some
            "Basic Kafka consumer using kafkaSource with Stream.take and fold-based draining"
        }
      , Cookbook.CookbookEntry::{ key = "streamly-producer"
        , title = "Produce Kafka messages with kafkaSink"
        , contentType = Cookbook.ContentType.SampleCode
        , topics = [ Cookbook.Topic.Streaming ]
        , packages = [ "hw-kafka-streamly", "hw-kafka-client", "streamly-core" ]
        , language = Schema.Language.Haskell
        , audience = Schema.DocAudience.User
        , location =
            Schema.DocLocation.LocalFile
              "hw-kafka-streamly-jitsurei/app/StreamlyProducer.hs"
        , description = Some
            "Produce messages using withKafkaProducer and kafkaSink fold"
        }
      , Cookbook.CookbookEntry::{ key = "error-handling"
        , title = "Handle Kafka consumer errors with skipNonFatal and throwLeft"
        , contentType = Cookbook.ContentType.Pattern
        , topics = [ Cookbook.Topic.Streaming, Cookbook.Topic.ErrorHandling ]
        , packages = [ "hw-kafka-streamly", "hw-kafka-client", "streamly-core" ]
        , language = Schema.Language.Haskell
        , audience = Schema.DocAudience.User
        , location =
            Schema.DocLocation.LocalFile
              "hw-kafka-streamly-jitsurei/app/ErrorHandling.hs"
        , description = Some
            "Three error handling patterns: skipNonFatal, skipNonFatalExcept with timeout detection, and throwLeft with exception catching"
        }
      , Cookbook.CookbookEntry::{ key = "transform-pipeline"
        , title = "Transform consumed Kafka records with mapValue and Bifunctor"
        , contentType = Cookbook.ContentType.Pattern
        , topics = [ Cookbook.Topic.Streaming ]
        , packages = [ "hw-kafka-streamly", "hw-kafka-client", "streamly-core" ]
        , language = Schema.Language.Haskell
        , audience = Schema.DocAudience.User
        , location =
            Schema.DocLocation.LocalFile
              "hw-kafka-streamly-jitsurei/app/TransformPipeline.hs"
        , description = Some
            "Transform record values with mapValue, keys with Bifunctor.first, and both with bimap"
        }
      , Cookbook.CookbookEntry::{ key = "batch-sink"
        , title = "Batch-produce Kafka messages with kafkaBatchSink"
        , contentType = Cookbook.ContentType.SampleCode
        , topics = [ Cookbook.Topic.Streaming, Cookbook.Topic.Performance ]
        , packages = [ "hw-kafka-streamly", "hw-kafka-client", "streamly-core" ]
        , language = Schema.Language.Haskell
        , audience = Schema.DocAudience.User
        , location =
            Schema.DocLocation.LocalFile
              "hw-kafka-streamly-jitsurei/app/BatchSink.hs"
        , description = Some
            "Batch messages with batchByOrFlush and produce via kafkaBatchSink"
        }
      , Cookbook.CookbookEntry::{ key = "consume-produce"
        , title = "Consume, transform, and produce Kafka messages in a pipeline"
        , contentType = Cookbook.ContentType.Pattern
        , topics = [ Cookbook.Topic.Streaming ]
        , packages = [ "hw-kafka-streamly", "hw-kafka-client", "streamly-core" ]
        , language = Schema.Language.Haskell
        , audience = Schema.DocAudience.User
        , location =
            Schema.DocLocation.LocalFile
              "hw-kafka-streamly-jitsurei/app/ConsumeProduce.hs"
        , description = Some
            "End-to-end pipeline: consume from one topic, transform, produce to another topic"
        }
      , Cookbook.CookbookEntry::{ key = "concurrent-consume"
        , title = "Process Kafka messages concurrently with parMapM"
        , contentType = Cookbook.ContentType.Pattern
        , topics = [ Cookbook.Topic.Streaming, Cookbook.Topic.Performance ]
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
