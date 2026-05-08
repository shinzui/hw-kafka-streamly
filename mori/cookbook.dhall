let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/1f70781427426c09673d46f8e6733b7e7d0abedc/package.dhall
        sha256:3b79aae9216456678300441ca8616b64a4b4fa520a1286dfcc418f60899d5d4a

let Cookbook =
      https://raw.githubusercontent.com/shinzui/mori-schema/1f70781427426c09673d46f8e6733b7e7d0abedc/extensions/cookbook/package.dhall
        sha256:5d41094fcc37d35ddef48af2e0401764d0ae77f9bd25127a979473b964affbb7

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
