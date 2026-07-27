module Kafka.Streamly.StreamTest (tests) where

import Kafka.Consumer (KafkaError (..), RdKafkaRespErrT (..))
import Kafka.Streamly.Stream (
    isFatal,
    isPartitionEOF,
    isPollTimeout,
    skipNonFatal,
    skipNonFatalExcept,
 )
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream (Stream)
import Streamly.Data.Stream qualified as Stream
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (Assertion, testCase, (@?=))

tests :: TestTree
tests =
    testGroup
        "Stream"
        [ testGroup "isFatal" isFatalTests
        , testGroup "isPollTimeout" isPollTimeoutTests
        , testGroup "isPartitionEOF" isPartitionEOFTests
        , testGroup "skipNonFatal" skipNonFatalTests
        , testGroup "skipNonFatalExcept" skipNonFatalExceptTests
        ]

-------------------------------------------------------------------------------
-- isFatal
-------------------------------------------------------------------------------

fatalErrors :: [(String, KafkaError)]
fatalErrors =
    [ ("KafkaUnknownConfigurationKey", KafkaUnknownConfigurationKey "")
    , ("KafkaInvalidConfigurationValue", KafkaInvalidConfigurationValue "")
    , ("KafkaBadConfiguration", KafkaBadConfiguration)
    , ("KafkaBadSpecification", KafkaBadSpecification "")
    , ("RdKafkaRespErrDestroy", KafkaResponseError RdKafkaRespErrDestroy)
    , ("RdKafkaRespErrFail", KafkaResponseError RdKafkaRespErrFail)
    , ("RdKafkaRespErrInvalidArg", KafkaResponseError RdKafkaRespErrInvalidArg)
    , ("RdKafkaRespErrSsl", KafkaResponseError RdKafkaRespErrSsl)
    , ("RdKafkaRespErrUnknownProtocol", KafkaResponseError RdKafkaRespErrUnknownProtocol)
    , ("RdKafkaRespErrNotImplemented", KafkaResponseError RdKafkaRespErrNotImplemented)
    , ("RdKafkaRespErrAuthentication", KafkaResponseError RdKafkaRespErrAuthentication)
    , ("RdKafkaRespErrInconsistentGroupProtocol", KafkaResponseError RdKafkaRespErrInconsistentGroupProtocol)
    , ("RdKafkaRespErrTopicAuthorizationFailed", KafkaResponseError RdKafkaRespErrTopicAuthorizationFailed)
    , ("RdKafkaRespErrGroupAuthorizationFailed", KafkaResponseError RdKafkaRespErrGroupAuthorizationFailed)
    , ("RdKafkaRespErrClusterAuthorizationFailed", KafkaResponseError RdKafkaRespErrClusterAuthorizationFailed)
    , ("RdKafkaRespErrUnsupportedSaslMechanism", KafkaResponseError RdKafkaRespErrUnsupportedSaslMechanism)
    , ("RdKafkaRespErrIllegalSaslState", KafkaResponseError RdKafkaRespErrIllegalSaslState)
    , ("RdKafkaRespErrUnsupportedVersion", KafkaResponseError RdKafkaRespErrUnsupportedVersion)
    , ("RdKafkaRespErrFatal", KafkaResponseError RdKafkaRespErrFatal)
    , ("RdKafkaRespErrSaslAuthenticationFailed", KafkaResponseError RdKafkaRespErrSaslAuthenticationFailed)
    ]

nonFatalErrors :: [(String, KafkaError)]
nonFatalErrors =
    [ ("RdKafkaRespErrTimedOut", KafkaResponseError RdKafkaRespErrTimedOut)
    , ("RdKafkaRespErrPartitionEof", KafkaResponseError RdKafkaRespErrPartitionEof)
    , ("RdKafkaRespErrNoError", KafkaResponseError RdKafkaRespErrNoError)
    ]

isFatalTests :: [TestTree]
isFatalTests =
    [ testCase ("fatal: " <> label) (isFatal err @?= True)
    | (label, err) <- fatalErrors
    ]
        <> [ testCase ("non-fatal: " <> label) (isFatal err @?= False)
           | (label, err) <- nonFatalErrors
           ]

-------------------------------------------------------------------------------
-- isPollTimeout
-------------------------------------------------------------------------------

isPollTimeoutTests :: [TestTree]
isPollTimeoutTests =
    [ testCase "matches RdKafkaRespErrTimedOut" $
        isPollTimeout (KafkaResponseError RdKafkaRespErrTimedOut) @?= True
    , testCase "rejects RdKafkaRespErrPartitionEof" $
        isPollTimeout (KafkaResponseError RdKafkaRespErrPartitionEof) @?= False
    , testCase "rejects RdKafkaRespErrAuthentication" $
        isPollTimeout (KafkaResponseError RdKafkaRespErrAuthentication) @?= False
    , testCase "rejects KafkaBadConfiguration" $
        isPollTimeout KafkaBadConfiguration @?= False
    ]

-------------------------------------------------------------------------------
-- isPartitionEOF
-------------------------------------------------------------------------------

isPartitionEOFTests :: [TestTree]
isPartitionEOFTests =
    [ testCase "matches RdKafkaRespErrPartitionEof" $
        isPartitionEOF (KafkaResponseError RdKafkaRespErrPartitionEof) @?= True
    , testCase "rejects RdKafkaRespErrTimedOut" $
        isPartitionEOF (KafkaResponseError RdKafkaRespErrTimedOut) @?= False
    , testCase "rejects RdKafkaRespErrAuthentication" $
        isPartitionEOF (KafkaResponseError RdKafkaRespErrAuthentication) @?= False
    , testCase "rejects KafkaBadConfiguration" $
        isPartitionEOF KafkaBadConfiguration @?= False
    ]

-------------------------------------------------------------------------------
-- skipNonFatal / skipNonFatalExcept
-------------------------------------------------------------------------------

runStream ::
    (Stream IO a -> Stream IO b) ->
    [a] ->
    IO [b]
runStream f xs = Stream.fold Fold.toList (f (Stream.fromList xs))

timedOut, partitionEof, authentication, librdkafkaFatal :: KafkaError
timedOut = KafkaResponseError RdKafkaRespErrTimedOut
partitionEof = KafkaResponseError RdKafkaRespErrPartitionEof
authentication = KafkaResponseError RdKafkaRespErrAuthentication
librdkafkaFatal = KafkaResponseError RdKafkaRespErrFatal

skipNonFatalTests :: [TestTree]
skipNonFatalTests =
    [ testCase "drops poll-timeout, keeps authentication, keeps all Rights" dropsAndKeeps
    , testCase "passes all Rights through unchanged" passesAllRights
    , testCase "drops non-fatal Lefts even with empty extension list" dropsNonFatal
    , testCase "drops partition-EOF (non-fatal)" dropsPartitionEof
    , testCase "keeps a librdkafka fatal error" keepsLibrdkafkaFatal
    ]
  where
    dropsAndKeeps :: Assertion
    dropsAndKeeps = do
        let input =
                [ Right (1 :: Int)
                , Left timedOut
                , Right 2
                , Left authentication
                , Right 3
                ]
        result <- runStream skipNonFatal input
        result @?= [Right 1, Right 2, Left authentication, Right 3]

    passesAllRights :: Assertion
    passesAllRights = do
        let input = [Right (n :: Int) | n <- [1 .. 5]]
        result <- runStream skipNonFatal input
        result @?= input

    dropsNonFatal :: Assertion
    dropsNonFatal = do
        let input :: [Either KafkaError Int]
            input =
                [ Left timedOut
                , Left partitionEof
                , Left (KafkaResponseError RdKafkaRespErrNoError)
                ]
        result <- runStream skipNonFatal input
        result @?= []

    dropsPartitionEof :: Assertion
    dropsPartitionEof = do
        let input =
                [ Right (1 :: Int)
                , Left partitionEof
                , Right 2
                ]
        result <- runStream skipNonFatal input
        result @?= [Right 1, Right 2]

    -- This is the regression that made 'skipNonFatal' silently discard the one
    -- signal that the consumer is permanently dead. 'RdKafkaRespErrFatal' is
    -- what librdkafka delivers in-band once a fatal error has been raised, so
    -- filtering it out leaves a stream that polls a dead consumer forever.
    --
    -- The defect can only be pinned at this level. The poll action inside
    -- 'kafkaStreamNoClose' is not injectable — it calls 'pollMessage' on a
    -- concrete 'KafkaConsumer' — so a fenced-consumer scenario cannot be
    -- simulated in a unit test without a real broker and a real consumer
    -- handle. The classification predicate is where the bug lived, and this is
    -- where it stays fixed.
    keepsLibrdkafkaFatal :: Assertion
    keepsLibrdkafkaFatal = do
        let input =
                [ Right (1 :: Int)
                , Left librdkafkaFatal
                , Right 2
                ]
        result <- runStream skipNonFatal input
        result @?= [Right 1, Left librdkafkaFatal, Right 2]

skipNonFatalExceptTests :: [TestTree]
skipNonFatalExceptTests =
    [ testCase "keeps poll-timeout when isPollTimeout is in the list" keepsTimeouts
    , testCase "keeps partition-EOF when isPartitionEOF is in the list" keepsPartitionEof
    , testCase "empty predicate list behaves like skipNonFatal" emptyListEquivalent
    ]
  where
    keepsTimeouts :: Assertion
    keepsTimeouts = do
        let input =
                [ Right (1 :: Int)
                , Left timedOut
                , Right 2
                , Left authentication
                , Right 3
                ]
        result <- runStream (skipNonFatalExcept [isPollTimeout]) input
        result @?= [Right 1, Left timedOut, Right 2, Left authentication, Right 3]

    keepsPartitionEof :: Assertion
    keepsPartitionEof = do
        let input =
                [ Right (1 :: Int)
                , Left partitionEof
                , Left timedOut
                , Right 2
                ]
        result <- runStream (skipNonFatalExcept [isPartitionEOF]) input
        result @?= [Right 1, Left partitionEof, Right 2]

    emptyListEquivalent :: Assertion
    emptyListEquivalent = do
        let input =
                [ Right (1 :: Int)
                , Left timedOut
                , Left authentication
                , Right 2
                ]
        viaExcept <- runStream (skipNonFatalExcept []) input
        viaPlain <- runStream skipNonFatal input
        viaExcept @?= viaPlain
