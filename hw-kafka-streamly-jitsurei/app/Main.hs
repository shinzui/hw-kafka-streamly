module Main (main) where

main :: IO ()
main = do
    putStrLn "hw-kafka-streamly-jitsurei: cookbook examples"
    putStrLn ""
    putStrLn "Available examples:"
    putStrLn "  streamly-producer     Basic streaming production via kafkaFold"
    putStrLn "  streamly-consumer     Basic streaming consumption via kafkaStream"
    putStrLn "  error-handling        Three error handling strategies"
    putStrLn "  transform-pipeline    Stream transformation with mapValue, bimapValue"
    putStrLn "  batch-fold            Batched production with batchByOrFlush"
    putStrLn "  consume-produce       ETL pipeline: consume, transform, produce"
    putStrLn "  concurrent-consume    Concurrent message processing with parMapM"
    putStrLn ""
    putStrLn "Run with: cabal run <example-name>"
