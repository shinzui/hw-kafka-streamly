{- | Deterministic-close tests for the scoped stream API.

This is the KSC-7 regression suite of MasterPlan 23. The defect: 'kafkaStream'
and 'kafkaStreamAutoClose' close the consumer with a stream-level
@bracketIO@, and a stream is /pulled/ by its consumer — so when the downstream
stops pulling (a @take@, an early-terminating fold, an exception thrown
downstream) there is no execution point left for the cleanup and streamly falls
back to a garbage-collector finalizer. Until some GC runs, hw-kafka-client's
background loop keeps polling, which keeps the group membership alive and the
partitions assigned to a consumer nobody is reading.

Every test here builds a real consumer against an unreachable broker. That is
not a stub: librdkafka connects lazily, so @newConsumer@ succeeds, the real
background poll loop starts, and the real 'closeConsumer' runs — only the
network is absent. Polls return timeouts, which is all the stream needs to
produce elements.

Note that nothing in this module calls @performGC@. Its absence is the point:
the close must have happened by the time the scope returns, with no collection
involved.
-}
module Kafka.Streamly.WithStreamTest (tests) where

import Control.Exception (Exception, finally, throwIO, try)
import Data.ByteString qualified as BS
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Data.Text (Text)
import Kafka.Consumer (
    BrokerAddress (..),
    ConsumerGroupId (..),
    ConsumerProperties,
    ConsumerRecord,
    KafkaConsumer,
    KafkaError,
    Subscription,
    Timeout (..),
    TopicName (..),
    brokersList,
    closeConsumer,
    groupId,
    newConsumer,
    topics,
 )
import Kafka.Streamly.Stream (kafkaStreamNoClose, withConsumerStreamVia)
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream (Stream)
import Streamly.Data.Stream qualified as Stream
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (Assertion, assertFailure, testCase, (@?=))

type Record = ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)

{- | Point at a port nothing listens on. Each test uses its own group id so the
suite's @-N@ parallelism cannot make them interfere.
-}
propsFor :: Text -> ConsumerProperties
propsFor group =
    brokersList [BrokerAddress "localhost:1"]
        <> groupId (ConsumerGroupId group)

subscription :: Subscription
subscription = topics [TopicName "with-stream-test-topic"]

-- | Short, so the timeout-yielding stream produces elements quickly.
pollTimeout :: Timeout
pollTimeout = Timeout 50

mkConsumer :: Text -> IO KafkaConsumer
mkConsumer group =
    newConsumer (propsFor group) subscription
        >>= either (\err -> assertFailure ("newConsumer failed: " <> show err)) pure

-- | A close action that counts its invocations and then really closes.
countingClose :: IORef Int -> KafkaConsumer -> IO ()
countingClose counter consumer = do
    modifyIORef' counter (+ 1)
    () <$ closeConsumer consumer

data Sentinel = Sentinel
    deriving stock (Show)

instance Exception Sentinel

tests :: TestTree
tests =
    testGroup
        "WithStream"
        [ testCase "closes the consumer on abandonment, before the scope returns" closesOnAbandonment
        , testCase "closes exactly once when the continuation throws" closesOnException
        , testCase "the legacy stream-level bracket defers close under take" legacyDefersClose
        ]

{- | The KSC-7 regression. Take two elements out of a stream that has more, then
let the scope end: the close must already have run.
-}
closesOnAbandonment :: Assertion
closesOnAbandonment = do
    closed <- newIORef (0 :: Int)
    consumer <- mkConsumer "with-stream-abandonment"
    taken <-
        withConsumerStreamVia
            (pure consumer)
            (countingClose closed)
            pollTimeout
            (Stream.fold Fold.toList . Stream.take 2)
    length (taken :: [Either KafkaError Record]) @?= 2
    n <- readIORef closed
    n @?= 1

-- | The bracket must also fire when the continuation raises, and exactly once.
closesOnException :: Assertion
closesOnException = do
    closed <- newIORef (0 :: Int)
    consumer <- mkConsumer "with-stream-exception"
    result <-
        try $
            withConsumerStreamVia
                (pure consumer)
                (countingClose closed)
                pollTimeout
                ( \stream -> do
                    _ <- Stream.fold Fold.toList (Stream.take 1 stream)
                    throwIO Sentinel
                )
    case result :: Either Sentinel [Either KafkaError Record] of
        Left Sentinel -> pure ()
        Right _ -> assertFailure "expected the sentinel exception to propagate"
    n <- readIORef closed
    n @?= 1

{- | Documents /why/ the scoped API exists, by reproducing the old shape
locally: a stream-level @bracketIO@ whose stream is abandoned by @take@.

The assertion is that the cleanup has __not__ run by the time the fold
returns. If a future streamly release makes @bracketIO@ prompt under
abandonment, this test will fail — that is a welcome outcome, not a bug. Record
it and revisit the warnings on 'Kafka.Streamly.Stream.kafkaStream' and
'Kafka.Streamly.Stream.kafkaStreamAutoClose'.
-}
legacyDefersClose :: Assertion
legacyDefersClose = do
    closed <- newIORef (0 :: Int)
    consumer <- mkConsumer "with-stream-legacy"
    ( do
            taken <- leakyTake consumer closed
            length taken @?= 2
            n <- readIORef closed
            n @?= 0
        )
        -- The consumer was never closed by the stream; close it here so this
        -- test does not leave a poll loop running for the rest of the suite.
        `finally` (() <$ closeConsumer consumer)

leakyTake :: KafkaConsumer -> IORef Int -> IO [Either KafkaError Record]
leakyTake consumer closed =
    Stream.fold Fold.toList
        . Stream.take 2
        $ legacyStream
  where
    legacyStream :: Stream IO (Either KafkaError Record)
    legacyStream =
        Stream.bracketIO
            (pure consumer)
            (countingClose closed)
            (\c -> kafkaStreamNoClose c pollTimeout)
