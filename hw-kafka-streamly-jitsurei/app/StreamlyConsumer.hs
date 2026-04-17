module Main (main) where

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
import Kafka.Streamly.Source (kafkaSource)
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream qualified as Stream

consumerProps :: ConsumerProperties
consumerProps =
    brokersList [defaultBrokerAddress]
        <> groupId (ConsumerGroupId "jitsurei-streamly-consumer")
        <> noAutoCommit
        <> logLevel KafkaLogInfo

consumerSub :: Subscription
consumerSub =
    topics [defaultTopicName]
        <> offsetReset Earliest

showBS :: Maybe BS.ByteString -> String
showBS = BS8.unpack . fromMaybe "<null>"

printMessage :: Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)) -> IO ()
printMessage (Left err) =
    putStrLn $ "  Error: " <> show err
printMessage (Right record) =
    putStrLn $
        "  Received: key="
            <> showBS (crKey record)
            <> " value="
            <> showBS (crValue record)
            <> " (offset="
            <> show (crOffset record)
            <> ")"

main :: IO ()
main = do
    putStrLn $ "Consuming up to 10 messages from " <> show defaultTopicName <> " via kafkaSource..."
    kafkaSource consumerProps consumerSub defaultTimeout
        & Stream.take 10
        & Stream.fold (Fold.drainMapM printMessage)
    putStrLn "Done."
