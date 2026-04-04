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

-- | Throws the left part of a value as an exception, passing right values through.
throwLeft ::
    (MonadThrow m, Exception e) =>
    Stream m (Either e a) ->
    Stream m a
throwLeft = Stream.mapMaybeM $ \case
    Left e -> throwM e
    Right a -> pure (Just a)
{-# INLINE throwLeft #-}

{- | Throws the left part of a value as an exception if it satisfies the predicate.
Non-matching left values and all right values pass through unchanged.
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
-}
batchByOrFlush ::
    (Monad m) =>
    BatchSize ->
    Stream m (Maybe a) ->
    Stream m [a]
batchByOrFlush n input = batchInternal n (Stream.append input (Stream.fromPure Nothing))
{-# INLINE batchByOrFlush #-}

{- | Batch stream elements by size, flushing on 'Left'.
'Right' values accumulate into a batch. When the batch reaches
the specified size or a 'Left' is received, the current batch is emitted.
Empty batches are not emitted. A final incomplete batch is emitted when the
stream ends.
-}
batchByOrFlushEither ::
    (Monad m) =>
    BatchSize ->
    Stream m (Either e a) ->
    Stream m [a]
batchByOrFlushEither n input =
    batchInternal n $
        Stream.append (fmap eitherToMaybe input) (Stream.fromPure Nothing)
  where
    eitherToMaybe (Left _) = Nothing
    eitherToMaybe (Right a) = Just a
{-# INLINE batchByOrFlushEither #-}

-- Internal batching implementation shared by both combinators.
batchInternal ::
    (Monad m) =>
    BatchSize ->
    Stream m (Maybe a) ->
    Stream m [a]
batchInternal (BatchSize n) =
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
