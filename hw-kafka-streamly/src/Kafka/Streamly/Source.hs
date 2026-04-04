module Kafka.Streamly.Source (
    -- * Stream sources
    kafkaSource,
    kafkaSourceAutoClose,
    kafkaSourceNoClose,

    -- * Error predicates
    isFatal,
    isPollTimeout,
    isPartitionEOF,

    -- * Error filters
    skipNonFatal,
    skipNonFatalExcept,

    -- * Value mapping
    mapFirst,
    mapValue,
    bimapValue,
    sequenceValueFirst,
    sequenceValue,
    bisequenceValue,
    traverseValueFirst,
    traverseValue,
    bitraverseValue,
    traverseValueFirstM,
    traverseValueM,
    bitraverseValueM,
) where

import Control.Exception (throwIO)
import Control.Monad.Catch (MonadCatch)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Bifunctor (Bifunctor, bimap, first)
import Data.Bitraversable (Bitraversable, bisequenceA, bitraverse)
import Data.ByteString qualified as BS
import Kafka.Consumer (
    ConsumerProperties,
    ConsumerRecord,
    KafkaConsumer,
    KafkaError (..),
    RdKafkaRespErrT (..),
    Subscription,
    Timeout,
    closeConsumer,
    newConsumer,
    pollMessage,
 )
import Streamly.Data.Stream (Stream)
import Streamly.Data.Stream qualified as Stream

{- | Create a 'Stream' for a given 'KafkaConsumer'.
The consumer will NOT be closed when the stream ends.
-}
kafkaSourceNoClose ::
    (MonadIO m) =>
    KafkaConsumer ->
    Timeout ->
    Stream m (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)))
kafkaSourceNoClose consumer timeout =
    Stream.unfoldrM step True
  where
    step False = pure Nothing
    step True = do
        msg <- liftIO $ pollMessage consumer timeout
        case msg of
            Left err | isFatal err -> pure $ Just (Left err, False)
            _ -> pure $ Just (msg, True)
{-# INLINE kafkaSourceNoClose #-}

{- | Create a 'Stream' for a given 'KafkaConsumer'.
The consumer will be closed automatically when the stream ends.
-}
kafkaSourceAutoClose ::
    (MonadIO m, MonadCatch m) =>
    KafkaConsumer ->
    Timeout ->
    Stream m (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)))
kafkaSourceAutoClose consumer timeout =
    Stream.bracketIO
        (pure consumer)
        (\c -> () <$ closeConsumer c)
        (\c -> kafkaSourceNoClose c timeout)
{-# INLINE kafkaSourceAutoClose #-}

{- | Create a 'Stream' that creates a consumer from properties, polls messages,
and closes the consumer when the stream ends. If consumer creation fails,
the 'KafkaError' is thrown as an exception.
-}
kafkaSource ::
    (MonadIO m, MonadCatch m) =>
    ConsumerProperties ->
    Subscription ->
    Timeout ->
    Stream m (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)))
kafkaSource props sub timeout =
    Stream.bracketIO
        ( newConsumer props sub >>= \case
            Left err -> throwIO err
            Right c -> pure c
        )
        (\c -> () <$ closeConsumer c)
        (\c -> kafkaSourceNoClose c timeout)
{-# INLINE kafkaSource #-}

-------------------------------------------------------------------------------
-- Error predicates
-------------------------------------------------------------------------------

{- | Checks if the error is fatal in a way that it doesn't make sense to retry
after or is unsafe to ignore.
-}
isFatal :: KafkaError -> Bool
isFatal = \case
    KafkaUnknownConfigurationKey _ -> True
    KafkaInvalidConfigurationValue _ -> True
    KafkaBadConfiguration -> True
    KafkaBadSpecification _ -> True
    KafkaResponseError RdKafkaRespErrDestroy -> True
    KafkaResponseError RdKafkaRespErrFail -> True
    KafkaResponseError RdKafkaRespErrInvalidArg -> True
    KafkaResponseError RdKafkaRespErrSsl -> True
    KafkaResponseError RdKafkaRespErrUnknownProtocol -> True
    KafkaResponseError RdKafkaRespErrNotImplemented -> True
    KafkaResponseError RdKafkaRespErrAuthentication -> True
    KafkaResponseError RdKafkaRespErrInconsistentGroupProtocol -> True
    KafkaResponseError RdKafkaRespErrTopicAuthorizationFailed -> True
    KafkaResponseError RdKafkaRespErrGroupAuthorizationFailed -> True
    KafkaResponseError RdKafkaRespErrClusterAuthorizationFailed -> True
    KafkaResponseError RdKafkaRespErrUnsupportedSaslMechanism -> True
    KafkaResponseError RdKafkaRespErrIllegalSaslState -> True
    KafkaResponseError RdKafkaRespErrUnsupportedVersion -> True
    _ -> False
{-# INLINE isFatal #-}

{- | Checks if the error is a poll timeout ('RdKafkaRespErrTimedOut').
Timeout errors are not fatal and occur when 'pollMessage' has no messages
to return within the specified timeout.
-}
isPollTimeout :: KafkaError -> Bool
isPollTimeout e = KafkaResponseError RdKafkaRespErrTimedOut == e
{-# INLINE isPollTimeout #-}

{- | Checks if the error indicates reaching the end of a partition
('RdKafkaRespErrPartitionEof'). Not fatal; occurs every time a consumer
reaches the current end of a partition.
-}
isPartitionEOF :: KafkaError -> Bool
isPartitionEOF e = KafkaResponseError RdKafkaRespErrPartitionEof == e
{-# INLINE isPartitionEOF #-}

-------------------------------------------------------------------------------
-- Error filters
-------------------------------------------------------------------------------

-- | Filter out non-fatal errors, keeping only fatal errors and successful records.
skipNonFatal ::
    (Monad m) =>
    Stream m (Either KafkaError b) ->
    Stream m (Either KafkaError b)
skipNonFatal = Stream.filter (either isFatal (const True))
{-# INLINE skipNonFatal #-}

{- | Filter out non-fatal errors except those matching any of the given predicates.
Fatal errors always pass through.

> skipNonFatalExcept [isPollTimeout, isPartitionEOF]
-}
skipNonFatalExcept ::
    (Monad m) =>
    [KafkaError -> Bool] ->
    Stream m (Either KafkaError b) ->
    Stream m (Either KafkaError b)
skipNonFatalExcept fs =
    let fun e = or $ (\f -> f e) <$> (isFatal : fs)
     in Stream.filter (either fun (const True))
{-# INLINE skipNonFatalExcept #-}

-------------------------------------------------------------------------------
-- Value mapping
-------------------------------------------------------------------------------

-- | Map over the first element (key) of a bifunctorial value.
mapFirst ::
    (Bifunctor t, Monad m) =>
    (k -> k') ->
    Stream m (t k v) ->
    Stream m (t k' v)
mapFirst f = fmap (first f)
{-# INLINE mapFirst #-}

-- | Map over the second element (value) of a functorial value.
mapValue ::
    (Functor t, Monad m) =>
    (v -> v') ->
    Stream m (t v) ->
    Stream m (t v')
mapValue f = fmap (fmap f)
{-# INLINE mapValue #-}

-- | Bimap over both elements of a bifunctorial value.
bimapValue ::
    (Bifunctor t, Monad m) =>
    (k -> k') ->
    (v -> v') ->
    Stream m (t k v) ->
    Stream m (t k' v')
bimapValue f g = fmap (bimap f g)
{-# INLINE bimapValue #-}

-- | Sequence the first element of a bitraversable value.
sequenceValueFirst ::
    (Bitraversable t, Applicative f, Monad m) =>
    Stream m (t (f k) v) ->
    Stream m (f (t k v))
sequenceValueFirst = fmap (bitraverse id pure)
{-# INLINE sequenceValueFirst #-}

-- | Sequence the value of a traversable.
sequenceValue ::
    (Traversable t, Applicative f, Monad m) =>
    Stream m (t (f v)) ->
    Stream m (f (t v))
sequenceValue = fmap sequenceA
{-# INLINE sequenceValue #-}

-- | Bisequence both elements of a bitraversable value.
bisequenceValue ::
    (Bitraversable t, Applicative f, Monad m) =>
    Stream m (t (f k) (f v)) ->
    Stream m (f (t k v))
bisequenceValue = fmap bisequenceA
{-# INLINE bisequenceValue #-}

-- | Traverse over the first element of a bitraversable value.
traverseValueFirst ::
    (Bitraversable t, Applicative f, Monad m) =>
    (k -> f k') ->
    Stream m (t k v) ->
    Stream m (f (t k' v))
traverseValueFirst f = fmap (bitraverse f pure)
{-# INLINE traverseValueFirst #-}

-- | Traverse over the value of a traversable.
traverseValue ::
    (Traversable t, Applicative f, Monad m) =>
    (v -> f v') ->
    Stream m (t v) ->
    Stream m (f (t v'))
traverseValue f = fmap (traverse f)
{-# INLINE traverseValue #-}

-- | Bitraverse over both elements of a bitraversable value.
bitraverseValue ::
    (Bitraversable t, Applicative f, Monad m) =>
    (k -> f k') ->
    (v -> f v') ->
    Stream m (t k v) ->
    Stream m (f (t k' v'))
bitraverseValue f g = fmap (bitraverse f g)
{-# INLINE bitraverseValue #-}

-- | Monadically traverse over the first element of a bitraversable value.
traverseValueFirstM ::
    (Bitraversable t, Applicative f, Monad m) =>
    (k -> m (f k')) ->
    Stream m (t k v) ->
    Stream m (f (t k' v))
traverseValueFirstM f = Stream.mapM (fmap (bitraverse id pure) . bitraverse f pure)
{-# INLINE traverseValueFirstM #-}

-- | Monadically traverse over a value.
traverseValueM ::
    (Traversable t, Applicative f, Monad m) =>
    (v -> m (f v')) ->
    Stream m (t v) ->
    Stream m (f (t v'))
traverseValueM f = Stream.mapM (fmap sequenceA . traverse f)
{-# INLINE traverseValueM #-}

-- | Monadically bitraverse over both elements of a bitraversable value.
bitraverseValueM ::
    (Bitraversable t, Applicative f, Monad m) =>
    (k -> m (f k')) ->
    (v -> m (f v')) ->
    Stream m (t k v) ->
    Stream m (f (t k' v'))
bitraverseValueM f g = Stream.mapM (fmap bisequenceA . bitraverse f g)
{-# INLINE bitraverseValueM #-}
