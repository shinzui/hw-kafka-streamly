{- |
Module      : Kafka.Streamly.Combinators
Description : Auxiliary combinators for Streamly–Kafka pipelines.

Auxiliary combinators that bridge consumer streams and producer folds:
size-bounded batching with an explicit flush token, and helpers for callers
who prefer exceptions over the @Either KafkaError a@ values that
'Kafka.Streamly.Source' yields.

Re-exports t'Kafka.Types.BatchSize' from "Kafka.Types" for convenience.
-}
module Kafka.Streamly.Combinators (
    -- * Types
    BatchSize (..),

    -- * Batching combinators
    batchByOrFlush,
    batchByOrFlushEither,

    -- * Error handling
    throwLeft,
    throwLeftSatisfy,
) where

import Control.Monad.Catch (Exception, MonadThrow, throwM)
import Kafka.Types (BatchSize (..))
import Streamly.Data.Scanl qualified as Scanl
import Streamly.Data.Stream (Stream)
import Streamly.Data.Stream qualified as Stream

{- | Throws the left part of a value as an exception, passing right values
through.

@since 0.1.0.0
-}
throwLeft ::
    (MonadThrow m, Exception e) =>
    Stream m (Either e a) ->
    Stream m a
throwLeft = Stream.mapMaybeM $ \case
    Left e -> throwM e
    Right a -> pure (Just a)
{-# INLINE throwLeft #-}

{- | Throws the left part of a value as an exception if it satisfies the
predicate. Non-matching left values and all right values pass through
unchanged.

@since 0.1.0.0
-}
throwLeftSatisfy ::
    (MonadThrow m, Exception e) =>
    (e -> Bool) ->
    Stream m (Either e a) ->
    Stream m (Either e a)
throwLeftSatisfy p = Stream.mapMaybeM $ \case
    Left e | p e -> throwM e
    v -> pure (Just v)
{-# INLINE throwLeftSatisfy #-}

{- | Batch stream elements by size, flushing on 'Nothing'.

Elements wrapped in 'Just' accumulate into a batch. When the batch reaches
the specified size or a 'Nothing' is received, the current batch is emitted.
Empty batches are not emitted. A final incomplete batch is emitted when the
stream ends.

Raises an 'error' if the t'BatchSize' is non-positive — @BatchSize 0@ would
emit a singleton batch for every element, which is never what the caller
wants.

@since 0.1.0.0
-}
batchByOrFlush ::
    (Monad m) =>
    BatchSize ->
    Stream m (Maybe a) ->
    Stream m [a]
batchByOrFlush n input =
    batchInternal "batchByOrFlush" n (Stream.append input (Stream.fromPure Nothing))
{-# INLINE batchByOrFlush #-}

{- | Batch stream elements by size, flushing on 'Left'.

'Right' values accumulate into a batch. When the batch reaches
the specified size or a 'Left' is received, the current batch is emitted.
Empty batches are not emitted. A final incomplete batch is emitted when the
stream ends.

Raises an 'error' if the t'BatchSize' is non-positive.

@since 0.1.0.0
-}
batchByOrFlushEither ::
    (Monad m) =>
    BatchSize ->
    Stream m (Either e a) ->
    Stream m [a]
batchByOrFlushEither n input =
    batchInternal "batchByOrFlushEither" n $
        Stream.append (fmap eitherToMaybe input) (Stream.fromPure Nothing)
  where
    eitherToMaybe (Left _) = Nothing
    eitherToMaybe (Right a) = Just a
{-# INLINE batchByOrFlushEither #-}

-- Internal batching implementation shared by both combinators.
batchInternal ::
    (Monad m) =>
    String ->
    BatchSize ->
    Stream m (Maybe a) ->
    Stream m [a]
batchInternal caller (BatchSize n)
    | n <= 0 = error (caller <> ": BatchSize must be positive, got " <> show n)
    | otherwise =
        Stream.catMaybes
            . fmap extract
            . Stream.scanl (Scanl.mkScanl step initial)
  where
    initial :: (Int, [a], Maybe [a])
    initial = (0, [], Nothing)

    extract :: (Int, [a], Maybe [a]) -> Maybe [a]
    extract (_, _, out) = out

    step :: (Int, [a], Maybe [a]) -> Maybe a -> (Int, [a], Maybe [a])
    step (_, acc, _) Nothing =
        if null acc
            then (0, [], Nothing)
            else (0, [], Just (reverse acc))
    step (i, acc, _) (Just a) =
        let acc' = a : acc
         in if i + 1 >= n
                then (0, [], Just (reverse acc'))
                else (i + 1, acc', Nothing)
