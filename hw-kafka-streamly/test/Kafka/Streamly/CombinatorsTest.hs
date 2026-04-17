module Kafka.Streamly.CombinatorsTest (tests) where

import Control.Exception (
    ErrorCall,
    Exception,
    SomeException,
    try,
 )
import Data.Either (rights)
import Data.Maybe (catMaybes)
import Kafka.Streamly.Combinators (
    BatchSize (..),
    batchByOrFlush,
    batchByOrFlushEither,
    throwLeft,
    throwLeftSatisfy,
 )
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream (Stream)
import Streamly.Data.Stream qualified as Stream
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (Assertion, assertFailure, testCase, (@?=))
import Test.Tasty.QuickCheck (Positive (..), Property, ioProperty, testProperty, (===))

tests :: TestTree
tests =
    testGroup
        "Combinators"
        [ testGroup "batchByOrFlush unit" batchByOrFlushUnitTests
        , testGroup "batchByOrFlush property" batchByOrFlushProperties
        , testGroup "batchByOrFlushEither unit" batchByOrFlushEitherUnitTests
        , testGroup "batchByOrFlushEither property" batchByOrFlushEitherProperties
        , testGroup "BatchSize validation" batchSizeValidationTests
        , testGroup "throwLeft" throwLeftTests
        , testGroup "throwLeftSatisfy" throwLeftSatisfyTests
        ]

-------------------------------------------------------------------------------
-- Helpers
-------------------------------------------------------------------------------

runBatchM :: BatchSize -> [Maybe Int] -> IO [[Int]]
runBatchM n xs =
    Stream.fold Fold.toList (batchByOrFlush n (Stream.fromList xs))

runBatchE :: BatchSize -> [Either () Char] -> IO [String]
runBatchE n xs =
    Stream.fold Fold.toList (batchByOrFlushEither n (Stream.fromList xs))

runStream :: (Stream IO a -> Stream IO b) -> [a] -> IO [b]
runStream f xs = Stream.fold Fold.toList (f (Stream.fromList xs))

-------------------------------------------------------------------------------
-- batchByOrFlush unit
-------------------------------------------------------------------------------

batchByOrFlushUnitTests :: [TestTree]
batchByOrFlushUnitTests =
    [ testCase "groups into full batch and trailing partial" $ do
        result <- runBatchM (BatchSize 3) [Just 1, Just 2, Just 3, Just 4, Just 5]
        result @?= [[1, 2, 3], [4, 5]]
    , testCase "Nothing flushes the current partial batch" $ do
        result <- runBatchM (BatchSize 3) [Just 1, Nothing, Just 2, Just 3]
        result @?= [[1], [2, 3]]
    , testCase "empty input produces no batches" $ do
        result <- runBatchM (BatchSize 3) []
        result @?= []
    , testCase "only-Nothing input produces no empty batches" $ do
        result <- runBatchM (BatchSize 3) [Nothing, Nothing, Nothing]
        result @?= []
    , testCase "exact fill then trailing partial" $ do
        result <- runBatchM (BatchSize 2) [Just 1, Just 2, Just 3]
        result @?= [[1, 2], [3]]
    , testCase "BatchSize 1 emits singletons" $ do
        result <- runBatchM (BatchSize 1) [Just 1, Just 2, Just 3]
        result @?= [[1], [2], [3]]
    ]

-------------------------------------------------------------------------------
-- batchByOrFlush QuickCheck properties
-------------------------------------------------------------------------------

batchByOrFlushProperties :: [TestTree]
batchByOrFlushProperties =
    [ testProperty "concat of batches equals catMaybes of input" prop_concatEqualsCatMaybes
    , testProperty "every emitted batch has 1 <= length <= BatchSize" prop_batchLengths
    ]
  where
    prop_concatEqualsCatMaybes :: Positive Int -> [Maybe Int] -> Property
    prop_concatEqualsCatMaybes (Positive n) xs = ioProperty $ do
        result <- runBatchM (BatchSize n) xs
        pure (concat result === catMaybes xs)

    prop_batchLengths :: Positive Int -> [Maybe Int] -> Property
    prop_batchLengths (Positive n) xs = ioProperty $ do
        result <- runBatchM (BatchSize n) xs
        pure $
            all (\b -> not (null b) && length b <= n) result
                === True

-------------------------------------------------------------------------------
-- batchByOrFlushEither unit
-------------------------------------------------------------------------------

batchByOrFlushEitherUnitTests :: [TestTree]
batchByOrFlushEitherUnitTests =
    [ testCase "Left flushes, Right fills the batch" $ do
        result <- runBatchE (BatchSize 2) [Right 'a', Right 'b', Left (), Right 'c']
        result @?= ["ab", "c"]
    , testCase "leading Left produces no empty batch" $ do
        result <- runBatchE (BatchSize 2) [Left (), Right 'a']
        result @?= ["a"]
    , testCase "empty input produces no batches" $ do
        result <- runBatchE (BatchSize 2) []
        result @?= []
    ]

-------------------------------------------------------------------------------
-- batchByOrFlushEither QuickCheck properties
-------------------------------------------------------------------------------

batchByOrFlushEitherProperties :: [TestTree]
batchByOrFlushEitherProperties =
    [ testProperty "concat of batches equals rights of input" prop_concatEqualsRights
    , testProperty "every emitted batch has 1 <= length <= BatchSize" prop_batchLengthsE
    ]
  where
    prop_concatEqualsRights :: Positive Int -> [Either () Int] -> Property
    prop_concatEqualsRights (Positive n) xs = ioProperty $ do
        result <- Stream.fold Fold.toList (batchByOrFlushEither (BatchSize n) (Stream.fromList xs))
        pure (concat result === rights xs)

    prop_batchLengthsE :: Positive Int -> [Either () Int] -> Property
    prop_batchLengthsE (Positive n) xs = ioProperty $ do
        result <- Stream.fold Fold.toList (batchByOrFlushEither (BatchSize n) (Stream.fromList xs))
        pure $
            all (\b -> not (null b) && length b <= n) result
                === True

-------------------------------------------------------------------------------
-- BatchSize validation
-------------------------------------------------------------------------------

batchSizeValidationTests :: [TestTree]
batchSizeValidationTests =
    [ testCase "batchByOrFlush errors on BatchSize 0" $
        assertErrors
            (Stream.fold Fold.toList (batchByOrFlush (BatchSize 0) (Stream.fromList [Just (1 :: Int)])))
    , testCase "batchByOrFlush errors on negative BatchSize" $
        assertErrors
            (Stream.fold Fold.toList (batchByOrFlush (BatchSize (-3)) (Stream.fromList [Just (1 :: Int)])))
    , testCase "batchByOrFlushEither errors on BatchSize 0" $
        assertErrors
            (Stream.fold Fold.toList (batchByOrFlushEither (BatchSize 0) (Stream.fromList [Right 'a' :: Either () Char])))
    ]
  where
    assertErrors :: (Show a) => IO a -> Assertion
    assertErrors action = do
        result <- try @ErrorCall action
        case result of
            Left _ -> pure ()
            Right v -> assertFailure $ "expected ErrorCall, got: " <> show v

-------------------------------------------------------------------------------
-- throwLeft / throwLeftSatisfy
-------------------------------------------------------------------------------

data TestErr = Good | Bad
    deriving stock (Eq, Show)

instance Exception TestErr

throwLeftTests :: [TestTree]
throwLeftTests =
    [ testCase "passes Rights through unchanged" $ do
        let input = [Right 1, Right 2, Right 3] :: [Either TestErr Int]
        result <- runStream throwLeft input
        result @?= [1, 2, 3]
    , testCase "throws on Left" $ do
        let input = [Right 1, Left Bad, Right 3] :: [Either TestErr Int]
        caught <- try @TestErr (runStream throwLeft input)
        caught @?= Left Bad
    , testCase "empty stream produces no exception" $ do
        let input = [] :: [Either TestErr Int]
        result <- runStream throwLeft input
        result @?= []
    ]

throwLeftSatisfyTests :: [TestTree]
throwLeftSatisfyTests =
    [ testCase "passes non-matching Lefts and all Rights through" $ do
        let input =
                [ Right 1
                , Left Good
                , Right 2
                ] ::
                    [Either TestErr Int]
        result <- runStream (throwLeftSatisfy (== Bad)) input
        result @?= input
    , testCase "throws only on matching Left" $ do
        let input =
                [ Right 1
                , Left Good
                , Right 2
                , Left Bad
                ] ::
                    [Either TestErr Int]
        caught <- try @SomeException (runStream (throwLeftSatisfy (== Bad)) input)
        case caught of
            Left _ -> pure ()
            Right v -> assertFailure $ "expected exception, got: " <> show v
    , testCase "empty stream produces no exception" $ do
        let input = [] :: [Either TestErr Int]
        result <- runStream (throwLeftSatisfy (== Bad)) input
        result @?= []
    ]
