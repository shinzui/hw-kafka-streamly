module Main (main) where

import Control.Concurrent (threadDelay)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BS8
import Data.Maybe (fromMaybe)
import HwKafkaStreamly.Jitsurei.Config (defaultBrokerAddress, defaultTimeout, defaultTopicName)
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
import Kafka.Streamly.Source (kafkaSource, skipNonFatal)
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream qualified as Stream
import Streamly.Data.Stream.Prelude qualified as StreamP

consumerProps :: ConsumerProperties
consumerProps =
    brokersList [defaultBrokerAddress]
        <> groupId (ConsumerGroupId "jitsurei-streamly-concurrent")
        <> noAutoCommit
        <> logLevel KafkaLogInfo

consumerSub :: Subscription
consumerSub =
    topics [defaultTopicName]
        <> offsetReset Earliest

showBS :: Maybe BS.ByteString -> String
showBS = BS8.unpack . fromMaybe "<null>"

processMessage :: ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString) -> IO ()
processMessage record = do
    putStrLn $
        "  [start] Processing: key="
            <> showBS (crKey record)
            <> " value="
            <> showBS (crValue record)
    -- Simulate work
    threadDelay 100000
    putStrLn $
        "  [done]  Processed: key="
            <> showBS (crKey record)

main :: IO ()
main = do
    putStrLn $ "Consuming from " <> show defaultTopicName <> " with concurrent processing..."
    putStrLn "  (maxThreads=4, maxBuffer=8)"
    let source =
            kafkaSource consumerProps consumerSub defaultTimeout
        -- Note: parMapM dispatches work across threads, so [start] and [done]
        -- lines below will not appear in input order. This is expected.
        pipeline =
            StreamP.parMapM (StreamP.maxThreads 4 . StreamP.maxBuffer 8) processMessage $
                Stream.mapMaybe (either (const Nothing) Just) $
                    Stream.take 10 $
                        skipNonFatal source
    Stream.fold Fold.drain pipeline
    putStrLn "Done."
