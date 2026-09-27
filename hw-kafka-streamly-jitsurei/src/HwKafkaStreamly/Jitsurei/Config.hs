module HwKafkaStreamly.Jitsurei.Config
  ( defaultBrokerAddress,
    defaultTimeout,
    defaultTopicName,
    outputTopicName,
  )
where

import Kafka.Types (BrokerAddress (..), Timeout (..), TopicName (..))

defaultBrokerAddress :: BrokerAddress
defaultBrokerAddress = BrokerAddress "localhost:9092"

defaultTimeout :: Timeout
defaultTimeout = Timeout 10000

defaultTopicName :: TopicName
defaultTopicName = TopicName "jitsurei-topic"

outputTopicName :: TopicName
outputTopicName = TopicName "jitsurei-streamly-output"
