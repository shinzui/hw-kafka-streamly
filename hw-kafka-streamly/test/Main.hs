module Main (main) where

import Kafka.Streamly.CombinatorsTest qualified as CombinatorsTest
import Kafka.Streamly.SourceTest qualified as SourceTest
import Test.Tasty (TestTree, defaultMain, testGroup)

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests =
    testGroup
        "hw-kafka-streamly"
        [ SourceTest.tests
        , CombinatorsTest.tests
        ]
