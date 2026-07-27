module Main (main) where

import Kafka.Streamly.CombinatorsTest qualified as CombinatorsTest
import Kafka.Streamly.StreamTest qualified as StreamTest
import Kafka.Streamly.WithStreamTest qualified as WithStreamTest
import Test.Tasty (TestTree, defaultMain, testGroup)

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests =
    testGroup
        "hw-kafka-streamly"
        [ StreamTest.tests
        , CombinatorsTest.tests
        , WithStreamTest.tests
        ]
