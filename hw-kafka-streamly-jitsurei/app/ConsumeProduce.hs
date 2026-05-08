module Main (main) where

import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BS8
import Data.Function ((&))
import Data.Maybe (fromMaybe)
import HwKafkaStreamly.Jitsurei.Config (defaultBrokerAddress, defaultTimeout, defaultTopicName, outputTopicName)
import Kafka.Consumer (
    ConsumerGroupId (..),
    ConsumerProperties,
    ConsumerRecord (..),
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
import Kafka.Producer qualified as P
import Kafka.Streamly.Combinators (throwLeft)
import Kafka.Streamly.Fold (kafkaFold, withKafkaProducer)
import Kafka.Streamly.Stream (kafkaStream, skipNonFatal)
import Streamly.Data.Stream qualified as Stream

consumerProps :: ConsumerProperties
consumerProps =
    brokersList [defaultBrokerAddress]
        <> groupId (ConsumerGroupId "jitsurei-streamly-consume-produce")
        <> noAutoCommit
        <> logLevel KafkaLogInfo

consumerSub :: Subscription
consumerSub =
    topics [defaultTopicName]
        <> offsetReset Earliest

producerProps :: P.ProducerProperties
producerProps =
    P.brokersList [defaultBrokerAddress]
        <> P.sendTimeout defaultTimeout
        <> P.logLevel P.KafkaLogInfo

showBS :: Maybe BS.ByteString -> String
showBS = BS8.unpack . fromMaybe "<null>"

toOutputRecord :: ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString) -> P.ProducerRecord
toOutputRecord record =
    P.ProducerRecord
        { P.prTopic = outputTopicName
        , P.prPartition = P.UnassignedPartition
        , P.prKey = crKey record
        , P.prValue = Just $ "[processed] " <> fromMaybe "" (crValue record)
        , P.prHeaders = mempty
        }

main :: IO ()
main = do
    putStrLn $
        "Consuming from "
            <> show defaultTopicName
            <> ", transforming, producing to "
            <> show outputTopicName
            <> "..."
    result <- withKafkaProducer producerProps $ \producer -> do
        let stream =
                kafkaStream consumerProps consumerSub defaultTimeout
            pipeline =
                skipNonFatal stream
                    & throwLeft
                    & Stream.take 5
                    & Stream.mapM
                        ( \record -> do
                            putStrLn $ "  Processing: " <> showBS (crValue record)
                            pure (toOutputRecord record)
                        )
        Stream.fold (kafkaFold producer) pipeline
    case result of
        Left err -> putStrLn $ "Failed to create producer: " <> show err
        Right Nothing -> putStrLn "Done. All transformed messages produced successfully."
        Right (Just err) -> putStrLn $ "Production error: " <> show err
