module Main (main) where

import Control.Exception (SomeException, catch)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BS8
import Data.Function ((&))
import Data.Maybe (fromMaybe)
import HwKafkaStreamly.Jitsurei.Config (defaultBrokerAddress, defaultTimeout, defaultTopicName)
import Kafka.Consumer (
    ConsumerGroupId (..),
    ConsumerProperties,
    ConsumerRecord (..),
    KafkaError,
    KafkaLogLevel (..),
    OffsetReset (..),
    Subscription,
    brokersList,
    groupId,
    logLevel,
    noAutoCommit,
    offsetReset,
    topics,
 )
import Kafka.Streamly.Combinators (throwLeft)
import Kafka.Streamly.Source (
    isPollTimeout,
    kafkaSource,
    skipNonFatal,
    skipNonFatalExcept,
 )
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream qualified as Stream

consumerProps :: ConsumerProperties
consumerProps =
    brokersList [defaultBrokerAddress]
        <> groupId (ConsumerGroupId "jitsurei-streamly-error-handling")
        <> noAutoCommit
        <> logLevel KafkaLogInfo

consumerSub :: Subscription
consumerSub =
    topics [defaultTopicName]
        <> offsetReset Earliest

showBS :: Maybe BS.ByteString -> String
showBS = BS8.unpack . fromMaybe "<null>"

printEither :: Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)) -> IO ()
printEither (Left err) = putStrLn $ "  Error: " <> show err
printEither (Right record) = printRecord record

printRecord :: ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString) -> IO ()
printRecord record =
    putStrLn $
        "  Record: key="
            <> showBS (crKey record)
            <> " value="
            <> showBS (crValue record)

main :: IO ()
main = do
    -- Pattern 1: skipNonFatal filters out timeouts and partition EOF
    putStrLn "=== Pattern 1: skipNonFatal ==="
    putStrLn "Consuming with skipNonFatal (only fatal errors and valid messages pass through)..."
    kafkaSource consumerProps consumerSub defaultTimeout
        & skipNonFatal
        & Stream.take 5
        & Stream.fold (Fold.drainMapM printEither)

    -- Pattern 2: skipNonFatalExcept to keep timeouts visible
    putStrLn ""
    putStrLn "=== Pattern 2: skipNonFatalExcept [isPollTimeout] ==="
    putStrLn "Consuming with timeout detection (useful to know when topic is drained)..."
    kafkaSource consumerProps consumerSub defaultTimeout
        & skipNonFatalExcept [isPollTimeout]
        & Stream.take 5
        & Stream.fold (Fold.drainMapM printEither)

    -- Pattern 3: throwLeft unwraps Either, throwing on error
    putStrLn ""
    putStrLn "=== Pattern 3: throwLeft with catch ==="
    putStrLn "Consuming with throwLeft (throws KafkaError as exception)..."
    ( kafkaSource consumerProps consumerSub defaultTimeout
            & skipNonFatal
            & throwLeft
            & Stream.take 5
            & Stream.fold (Fold.drainMapM printRecord)
        )
        `catch` (\(e :: SomeException) -> putStrLn $ "  Caught exception: " <> show e)

    putStrLn ""
    putStrLn "Done."
