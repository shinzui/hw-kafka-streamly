{- |
Module      : Kafka.Streamly.Stream
Description : Consumer streams and helpers for Streamly–Kafka pipelines.

Streams backed by a @hw-kafka-client@ 'KafkaConsumer'. Each stream
yields @Either t'Kafka.Consumer.KafkaError' ('ConsumerRecord' (Maybe ByteString) (Maybe ByteString))@ —
errors are kept in-band rather than raised as exceptions, matching the shape
exposed by 'Kafka.Consumer.pollMessage'.

== Three tiers of resource management

* 'kafkaStream' — fully managed: creates a consumer, polls, closes on stream
  termination. If consumer creation fails the t'KafkaError' is thrown as an
  exception (see the function's docstring for how this differs from
  @hw-kafka-conduit@).
* 'kafkaStreamAutoClose' — takes a pre-built consumer, polls, closes on
  termination. The caller owns creation, the stream owns destruction.
* 'kafkaStreamNoClose' — polls a consumer the caller fully owns. The stream
  never touches the consumer's lifecycle.

== Worked example

Consume five records from a topic, skipping non-fatal errors:

> import Kafka.Consumer
>     ( ConsumerGroupId (..), ConsumerRecord, KafkaError
>     , OffsetReset (..), brokersList, groupId, offsetReset, topics
>     )
> import Kafka.Streamly.Stream (kafkaStream, skipNonFatal)
> import Streamly.Data.Fold qualified as Fold
> import Streamly.Data.Stream qualified as Stream
>
> main :: IO ()
> main = do
>     let props = brokersList ["localhost:9092"]
>             <> groupId (ConsumerGroupId "example-group")
>         sub  = topics ["example-topic"] <> offsetReset Earliest
>     Stream.fold (Fold.drainMapM print) $
>         Stream.take 5 $
>             skipNonFatal $
>                 kafkaStream props sub (Timeout 1000)

== Transforming the stream

Because the element type is @Either KafkaError (ConsumerRecord k v)@, the
idiomatic way to map the inner record value is a double @fmap@:

> fmap (fmap (\\r -> r { crValue = Nothing })) stream

The helpers 'mapFirst', 'mapValue', and 'bimapValue' save one level of wrapping
by lifting a function directly into the outer stream element — see their
docstrings.
-}
module Kafka.Streamly.Stream (
    -- * Streams
    kafkaStream,
    kafkaStreamAutoClose,
    kafkaStreamNoClose,

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
) where

import Control.Exception (throwIO)
import Control.Monad.Catch (MonadCatch)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Bifunctor (Bifunctor, bimap, first)
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

The consumer will /not/ be closed when the stream ends; the caller owns its
lifecycle. The stream terminates the first time 'pollMessage' returns a fatal
t'KafkaError' (see 'isFatal'); non-fatal errors are yielded in-band as @Left@
values and polling continues.

@since 0.1.0.0
-}
kafkaStreamNoClose ::
    (MonadIO m) =>
    KafkaConsumer ->
    Timeout ->
    Stream m (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)))
kafkaStreamNoClose consumer timeout =
    Stream.unfoldrM step True
  where
    step False = pure Nothing
    step True = do
        msg <- liftIO $ pollMessage consumer timeout
        case msg of
            Left err | isFatal err -> pure $ Just (Left err, False)
            _ -> pure $ Just (msg, True)
{-# INLINE kafkaStreamNoClose #-}

{- | Create a 'Stream' for a given 'KafkaConsumer'.

The consumer is passed in by the caller but will be closed automatically when
the stream ends (via 'Streamly.Data.Stream.bracketIO'). Use this when you want
the stream to own destruction but not creation.

@since 0.1.0.0
-}
kafkaStreamAutoClose ::
    (MonadIO m, MonadCatch m) =>
    KafkaConsumer ->
    Timeout ->
    Stream m (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)))
kafkaStreamAutoClose consumer timeout =
    Stream.bracketIO
        (pure consumer)
        (\c -> () <$ closeConsumer c)
        (\c -> kafkaStreamNoClose c timeout)
{-# INLINE kafkaStreamAutoClose #-}

{- | Create a fully managed consumer 'Stream' from 'ConsumerProperties' and a
'Subscription'.

The stream creates the consumer on first demand, polls messages, and closes
the consumer when the stream ends.

If consumer creation fails, the t'KafkaError' is thrown as an exception (via
'throwIO'). This diverges from @hw-kafka-conduit@'s @kafkaSource@, which
yields @Left err@ as the first stream value and then terminates. Callers
migrating from conduit who prefer the in-band error should create the
consumer manually with 'Kafka.Consumer.newConsumer' and, on @Right c@, pass
@c@ to 'kafkaStreamAutoClose'.

Per-poll errors (non-fatal or fatal) are yielded in-band as @Left@ values,
as with the other streams in this module.

@since 0.1.0.0
-}
kafkaStream ::
    (MonadIO m, MonadCatch m) =>
    ConsumerProperties ->
    Subscription ->
    Timeout ->
    Stream m (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)))
kafkaStream props sub timeout =
    Stream.bracketIO
        ( newConsumer props sub >>= \case
            Left err -> throwIO err
            Right c -> pure c
        )
        (\c -> () <$ closeConsumer c)
        (\c -> kafkaStreamNoClose c timeout)
{-# INLINE kafkaStream #-}

-------------------------------------------------------------------------------
-- Error predicates
-------------------------------------------------------------------------------

{- | Checks if the error is fatal in a way that it doesn't make sense to retry
after or is unsafe to ignore.

@since 0.1.0.0
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

@since 0.1.0.0
-}
isPollTimeout :: KafkaError -> Bool
isPollTimeout e = KafkaResponseError RdKafkaRespErrTimedOut == e
{-# INLINE isPollTimeout #-}

{- | Checks if the error indicates reaching the end of a partition
('RdKafkaRespErrPartitionEof'). Not fatal; occurs every time a consumer
reaches the current end of a partition.

@since 0.1.0.0
-}
isPartitionEOF :: KafkaError -> Bool
isPartitionEOF e = KafkaResponseError RdKafkaRespErrPartitionEof == e
{-# INLINE isPartitionEOF #-}

-------------------------------------------------------------------------------
-- Error filters
-------------------------------------------------------------------------------

{- | Filter out non-fatal errors, keeping only fatal errors and successful
records.

This drops every t'KafkaError' for which 'isFatal' returns 'False', which
includes poll timeouts ('RdKafkaRespErrTimedOut') /and/ partition-EOF markers
('RdKafkaRespErrPartitionEof'). If your consumer relies on observing EOFs —
for example, to terminate a bounded read — use 'skipNonFatalExcept' with
'isPartitionEOF' instead.

@since 0.1.0.0
-}
skipNonFatal ::
    (Monad m) =>
    Stream m (Either KafkaError b) ->
    Stream m (Either KafkaError b)
skipNonFatal = Stream.filter (either isFatal (const True))
{-# INLINE skipNonFatal #-}

{- | Filter out non-fatal errors except those matching any of the given
predicates. Fatal errors always pass through.

> skipNonFatalExcept [isPollTimeout, isPartitionEOF]

@since 0.1.0.0
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

{- | Lift a function into the left position of a 'Bifunctor' stream element.

Given a stream of @t k v@ (for example @Either KafkaError (ConsumerRecord k v)@),
transform the left component. To map the key of a 'ConsumerRecord' inside an
@Either@ the caller still has to compose with an outer 'fmap':

> mapFirst (fmap prefixKey) :: Stream m (Either e (ConsumerRecord k v)) -> ...

@since 0.1.0.0
-}
mapFirst ::
    (Bifunctor t, Monad m) =>
    (k -> k') ->
    Stream m (t k v) ->
    Stream m (t k' v)
mapFirst f = fmap (first f)
{-# INLINE mapFirst #-}

{- | Lift a function into the 'Functor' position of a stream element.

For @Stream m (Either e (ConsumerRecord k v))@, @mapValue f@ rewrites the
@Right@ side, leaving errors untouched. To transform the record /value/
itself the caller composes with another 'fmap':

> mapValue (fmap uppercase) :: Stream m (Either e (ConsumerRecord k v)) -> ...

@since 0.1.0.0
-}
mapValue ::
    (Functor t, Monad m) =>
    (v -> v') ->
    Stream m (t v) ->
    Stream m (t v')
mapValue f = fmap (fmap f)
{-# INLINE mapValue #-}

{- | Lift a pair of functions into the two positions of a 'Bifunctor' stream
element.

Acts on the outer element. For @Stream m (Either KafkaError x)@ this maps
both the error and the success:

> bimapValue renderError handleSuccess
>     :: Stream m (Either KafkaError x) -> Stream m (Either String y)

To bimap the key and value /inside/ an outer 'Either', wrap with 'fmap':

> mapValue (bimap prefixKey uppercase)
>     :: Stream m (Either e (ConsumerRecord k v))
>     -> Stream m (Either e (ConsumerRecord k' v'))

@since 0.1.0.0
-}
bimapValue ::
    (Bifunctor t, Monad m) =>
    (k -> k') ->
    (v -> v') ->
    Stream m (t k v) ->
    Stream m (t k' v')
bimapValue f g = fmap (bimap f g)
{-# INLINE bimapValue #-}
