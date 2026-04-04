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
import Kafka.Streamly.Combinators (BatchSize (..), batchByOrFlush)
import Kafka.Streamly.Sink (kafkaBatchSink, withKafkaProducer)
import Streamly.Data.Fold qualified as Fold
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
        , prKey = Just $ BS8.pack $ "batch-key-" <> show n
        , prValue = Just $ BS8.pack $ "Batch message #" <> show n
        , prHeaders = mempty
        }

main :: IO ()
main = do
    putStrLn $ "Batch-producing 9 messages (batch size 3) to " <> show defaultTopicName <> "..."
    let messages = Stream.fromList (map (Just . mkRecord) [1 .. 9 :: Int])
        batched = batchByOrFlush (BatchSize 3) messages
    result <- withKafkaProducer producerProps $ \producer ->
        Stream.fold (kafkaBatchSink producer) batched
    case result of
        Left err -> putStrLn $ "Failed to create producer: " <> show err
        Right Nothing -> putStrLn "Done. All batches produced successfully."
        Right (Just err) -> putStrLn $ "Production error: " <> show err
