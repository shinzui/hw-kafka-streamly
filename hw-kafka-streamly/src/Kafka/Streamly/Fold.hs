-- |
-- Module      : Kafka.Streamly.Fold
-- Description : Producer folds and bracket helper for Streamly–Kafka pipelines.
--
-- Streamly expresses consumers of a stream as 'Fold' values, the dual of
-- @Streamly.Data.Stream.Stream@. This module provides folds that send each incoming
-- 'Kafka.Producer.ProducerRecord' to a Kafka broker, plus a bracket helper
-- ('withKafkaProducer') for the producer's lifecycle.
--
-- == Worked example
--
-- Fold five records through 'kafkaFold':
--
-- > import Kafka.Producer (ProducerRecord (..), TopicName (..), ProducePartition (..))
-- > import Kafka.Streamly.Fold (kafkaFold, withKafkaProducer)
-- > import Streamly.Data.Fold qualified as Fold
-- > import Streamly.Data.Stream qualified as Stream
-- >
-- > mkRecord :: Int -> ProducerRecord
-- > mkRecord i = ProducerRecord
-- >     { prTopic     = TopicName "example-topic"
-- >     , prPartition = UnassignedPartition
-- >     , prKey       = Nothing
-- >     , prValue     = Just (encodeUtf8 (pack ("payload-" <> show i)))
-- >     , prHeaders   = mempty
-- >     }
-- >
-- > main :: IO ()
-- > main = do
-- >     result <- withKafkaProducer producerProps $ \\producer ->
-- >         Stream.fold (kafkaFold producer) (Stream.fromList (map mkRecord [1..5]))
-- >     print result  -- Right Nothing on success
--
-- == Delivery semantics
--
-- A successful fold (@Nothing@) means every 'Kafka.Producer.produceMessage' call
-- returned without an error — which in librdkafka terms means the records were
-- /queued/ for delivery, not that the broker has acknowledged them. End-to-end
-- delivery requires a flush; 'withKafkaProducer' handles this on exit, or call
-- 'Kafka.Producer.flushProducer' explicitly.
module Kafka.Streamly.Fold
  ( -- * Producer folds
    kafkaFold,
    kafkaBatchFold,

    -- * Resource management
    withKafkaProducer,
  )
where

import Control.Exception (bracket)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Kafka.Producer
  ( KafkaError,
    KafkaProducer,
    ProducerProperties,
    ProducerRecord,
    closeProducer,
    newProducer,
    produceMessage,
  )
import Streamly.Data.Fold (Fold)
import Streamly.Data.Fold qualified as Fold

-- | A 'Fold' that sends each 'ProducerRecord' to Kafka via the given producer.
--
-- Returns 'Nothing' if every 'Kafka.Producer.produceMessage' call succeeded, or
-- 'Just' the first t'KafkaError' encountered. After an error the fold still
-- consumes the remaining input but sends nothing.
--
-- A 'Nothing' result means librdkafka has /queued/ all records for delivery; it
-- does not mean the broker has acknowledged them. For end-to-end delivery, run
-- the fold inside 'withKafkaProducer' (which flushes on exit) or call
-- 'Kafka.Producer.flushProducer' directly.
--
-- @since 0.1.0.0
kafkaFold ::
  (MonadIO m) =>
  KafkaProducer ->
  Fold m ProducerRecord (Maybe KafkaError)
kafkaFold producer = Fold.foldlM' step (pure Nothing)
  where
    step Nothing record = liftIO $ produceMessage producer record
    step err@(Just _) _ = pure err
{-# INLINE kafkaFold #-}

-- | A 'Fold' that sends batches of 'ProducerRecord' to Kafka.
--
-- Returns 'Nothing' if every record in every batch was accepted by
-- 'Kafka.Producer.produceMessage', or 'Just' the first t'KafkaError'. After an
-- error the fold still consumes remaining input but sends nothing.
--
-- As with 'kafkaFold', 'Nothing' means records are queued in librdkafka, not
-- acknowledged by the broker — use 'withKafkaProducer' or
-- 'Kafka.Producer.flushProducer' for delivery guarantees.
--
-- Note: each batch is sent as individual 'Kafka.Producer.produceMessage' calls,
-- so this fold __does not reduce network round-trips__ compared to 'kafkaFold'.
-- It is a convenience for accepting @[ProducerRecord]@ input — for example the
-- output of 'Kafka.Streamly.Combinators.batchByOrFlush' — and nothing more.
--
-- This is not a limitation of one @hw-kafka-client@ release. The package exports
-- no batch produce at all: it removed its Haskell-level @produceMessageBatch@ in
-- 72e6f6d (October 2021, before @v5.3.0@), and that function was itself a @mapM@
-- over @produceMessage@, so it would not have helped either. A true broker-side
-- batch send needs a binding for librdkafka's @rd_kafka_produce_batch@, which
-- the package has never had. Tracked as upstream issue
-- @hw-kafka-client-no-produce-batch-binding@ — run
-- @mori upstream-issues show hw-kafka-client-no-produce-batch-binding@ — and this
-- fold will switch to a real batch send if that binding ever lands.
--
-- @since 0.1.0.0
kafkaBatchFold ::
  (MonadIO m) =>
  KafkaProducer ->
  Fold m [ProducerRecord] (Maybe KafkaError)
kafkaBatchFold producer = Fold.foldlM' step (pure Nothing)
  where
    step Nothing batch = sendBatch batch
    step err@(Just _) _ = pure err

    sendBatch [] = pure Nothing
    sendBatch (r : rs) = do
      result <- liftIO $ produceMessage producer r
      case result of
        Nothing -> sendBatch rs
        Just err -> pure (Just err)
{-# INLINE kafkaBatchFold #-}

-- | Bracket producer creation and destruction around an action.
--
-- Creates a producer from the given 'ProducerProperties', passes it to @action@,
-- then closes the producer on exit. Closing a @hw-kafka-client@ producer flushes
-- any queued records before releasing resources, so callers do not need to flush
-- explicitly. Returns 'Left' if producer creation fails, otherwise 'Right' with
-- the action's result.
--
-- @since 0.1.0.0
withKafkaProducer ::
  ProducerProperties ->
  (KafkaProducer -> IO a) ->
  IO (Either KafkaError a)
withKafkaProducer props action =
  newProducer props >>= \case
    Left err -> pure (Left err)
    Right producer ->
      bracket
        (pure producer)
        closeProducer
        (\p -> Right <$> action p)
