module Main (main) where

import Data.Bifunctor (bimap, first)
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
import Kafka.Streamly.Source (kafkaSource, mapValue, skipNonFatal)
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream qualified as Stream

consumerProps :: ConsumerProperties
consumerProps =
    brokersList [defaultBrokerAddress]
        <> groupId (ConsumerGroupId "jitsurei-streamly-transform")
        <> noAutoCommit
        <> logLevel KafkaLogInfo

consumerSub :: Subscription
consumerSub =
    topics [defaultTopicName]
        <> offsetReset Earliest

showBS :: Maybe BS.ByteString -> String
showBS = BS8.unpack . fromMaybe "<null>"

uppercaseBS :: Maybe BS.ByteString -> Maybe BS.ByteString
uppercaseBS = fmap (BS8.map toUpperChar)
  where
    toUpperChar c
        | c >= 'a' && c <= 'z' = toEnum (fromEnum c - 32)
        | otherwise = c

prefixKey :: Maybe BS.ByteString -> Maybe BS.ByteString
prefixKey = fmap ("transformed-" <>)

printRecord :: ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString) -> IO ()
printRecord record =
    putStrLn $
        "  key="
            <> showBS (crKey record)
            <> " value="
            <> showBS (crValue record)

printEither :: Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)) -> IO ()
printEither (Left err) = putStrLn $ "  Error: " <> show err
printEither (Right record) = printRecord record

main :: IO ()
main = do
    -- Pattern 1: mapValue transforms the value field of ConsumerRecord
    putStrLn "=== Pattern 1: mapValue (uppercase record values) ==="
    kafkaSource consumerProps consumerSub defaultTimeout
        & skipNonFatal
        & mapValue (fmap uppercaseBS)
        & Stream.take 5
        & Stream.fold (Fold.drainMapM printEither)

    -- Pattern 2: fmap with Bifunctor.first to transform record keys
    putStrLn ""
    putStrLn "=== Pattern 2: fmap with Bifunctor.first (prefix record keys) ==="
    kafkaSource consumerProps consumerSub defaultTimeout
        & skipNonFatal
        & fmap (fmap (first prefixKey))
        & Stream.take 5
        & Stream.fold (Fold.drainMapM printEither)

    -- Pattern 3: fmap with bimap to transform both key and value
    putStrLn ""
    putStrLn "=== Pattern 3: fmap with bimap (prefix keys + uppercase values) ==="
    kafkaSource consumerProps consumerSub defaultTimeout
        & skipNonFatal
        & fmap (fmap (bimap prefixKey uppercaseBS))
        & Stream.take 5
        & Stream.fold (Fold.drainMapM printEither)

    putStrLn ""
    putStrLn "Done."
