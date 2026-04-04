module Kafka.Streamly.Sink (
    -- * Producer folds
    kafkaSink,
    kafkaBatchSink,

    -- * Resource management
    withKafkaProducer,
) where

import Control.Exception (bracket)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Kafka.Producer (
    KafkaError,
    KafkaProducer,
    ProducerProperties,
    ProducerRecord,
    closeProducer,
    flushProducer,
    newProducer,
    produceMessage,
 )
import Streamly.Data.Fold (Fold)
import Streamly.Data.Fold qualified as Fold

{- | A 'Fold' that sends each 'ProducerRecord' to Kafka via the given producer.
Returns 'Nothing' if all messages were sent successfully, or 'Just' the first
error encountered. After an error, remaining elements are skipped.
-}
kafkaSink ::
    (MonadIO m) =>
    KafkaProducer ->
    Fold m ProducerRecord (Maybe KafkaError)
kafkaSink producer = Fold.foldlM' step (pure Nothing)
  where
    step Nothing record = liftIO $ produceMessage producer record
    step err@(Just _) _ = pure err
{-# INLINE kafkaSink #-}

{- | A 'Fold' that sends batches of 'ProducerRecord' to Kafka.
Returns 'Nothing' if all messages in all batches were sent successfully,
or 'Just' the first error encountered.
-}
kafkaBatchSink ::
    (MonadIO m) =>
    KafkaProducer ->
    Fold m [ProducerRecord] (Maybe KafkaError)
kafkaBatchSink producer = Fold.foldlM' step (pure Nothing)
  where
    step Nothing batch = sendBatch batch
    step err@(Just _) _ = pure err

    sendBatch [] = pure Nothing
    sendBatch (r : rs) = do
        result <- liftIO $ produceMessage producer r
        case result of
            Nothing -> sendBatch rs
            Just err -> pure (Just err)
{-# INLINE kafkaBatchSink #-}

{- | Bracket producer creation and destruction around an action.
Creates a producer, passes it to the action, then flushes and closes the
producer. Returns 'Left' if producer creation fails, otherwise 'Right'
with the action's result.
-}
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
                (\p -> flushProducer p >> closeProducer p)
                (\p -> Right <$> action p)
