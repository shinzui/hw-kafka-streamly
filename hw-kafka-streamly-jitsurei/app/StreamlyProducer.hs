module Main (main) where

import Data.ByteString.Char8 qualified as BS8
import HwKafkaStreamly.Jitsurei.Config (defaultBrokerAddress, defaultTimeout, defaultTopicName)
import Kafka.Producer (
    KafkaLogLevel (..),
    ProducePartition (..),
    ProducerProperties,
    ProducerRecord (..),
    brokersList,
    logLevel,
    sendTimeout,
 )
import Kafka.Streamly.Fold (kafkaFold, withKafkaProducer)
import Streamly.Data.Stream qualified as Stream

producerProps :: ProducerProperties
producerProps =
    brokersList [defaultBrokerAddress]
        <> sendTimeout defaultTimeout
        <> logLevel KafkaLogInfo

mkRecord :: Int -> ProducerRecord
mkRecord n =
    ProducerRecord
        { prTopic = defaultTopicName
        , prPartition = UnassignedPartition
        , prKey = Just $ BS8.pack $ "key-" <> show n
        , prValue = Just $ BS8.pack $ "Hello from streamly producer #" <> show n
        , prHeaders = mempty
        }

main :: IO ()
main = do
    putStrLn $ "Producing 5 messages to " <> show defaultTopicName <> " via kafkaFold..."
    result <- withKafkaProducer producerProps $ \producer -> do
        let records = Stream.fromList (map mkRecord [1 .. 5 :: Int])
        Stream.fold (kafkaFold producer) records
    case result of
        Left err -> putStrLn $ "Failed to create producer: " <> show err
        Right Nothing -> putStrLn "Done. All messages produced successfully."
        Right (Just err) -> putStrLn $ "Production error: " <> show err
