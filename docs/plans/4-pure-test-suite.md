---
id: 4
slug: pure-test-suite
title: "Pure test suite"
kind: exec-plan
created_at: 2026-04-17T21:36:51Z
intention: "intention_01knbcpkxqemdaawn3zzs822f8"
master_plan: "docs/masterplans/1-release-hw-kafka-streamly-0.1.0.0.md"
---


# Pure test suite

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

After this plan is complete, `cabal test` produces a green test suite for the `hw-kafka-streamly` library. The suite exercises every pure function in the library: error predicates (`isFatal`, `isPollTimeout`, `isPartitionEOF`), error filters (`skipNonFatal`, `skipNonFatalExcept`), batching combinators (`batchByOrFlush`, `batchByOrFlushEither`), and exception combinators (`throwLeft`, `throwLeftSatisfy`). No Kafka broker is needed — these are pure stream transformations and predicates over synthetic input. A contributor who has just cloned the repository can run `cabal test hw-kafka-streamly` and see roughly 40 to 60 test cases pass within a second.

The suite uses `tasty` with `tasty-hunit` for unit tests and `tasty-quickcheck` for a few property tests on the batching combinators (which have natural invariants: concatenating the batches returns the original elements; no batch exceeds `BatchSize`; flush signals always close the current batch). Test failures print actionable messages — expected vs. actual, stream input that triggered the failure, predicate name.

This is the first test suite in the project. It establishes the convention other plans (and future releases) will build on.


## Progress

- [x] Add test-suite stanza to hw-kafka-streamly/hw-kafka-streamly.cabal (2026-04-17)
- [x] Create hw-kafka-streamly/test/Main.hs wiring the tasty tree (2026-04-17)
- [x] Create hw-kafka-streamly/test/Kafka/Streamly/SourceTest.hs with error predicate and filter tests (2026-04-17)
- [x] Create hw-kafka-streamly/test/Kafka/Streamly/CombinatorsTest.hs with batching and throw-on-error tests (2026-04-17)
- [x] Run cabal test hw-kafka-streamly and observe all tests pass — 58/58 green, ~0.00s (2026-04-17)
- [x] Verify cabal check still passes (2026-04-17)
- [x] Append a "Tests" bullet to CHANGELOG.md under 0.1.0.0 (2026-04-17)
- [x] Commit with ExecPlan + MasterPlan + Intention trailers — commit 7768957 (2026-04-17)


## Surprises & Discoveries

- EP-3 had already landed the `BatchSize` validation guard in
  `batchInternal` before EP-4 began, so the conditional test in the plan
  ("if EP-3 lands first, ...") was included unconditionally — three unit
  cases assert that `batchByOrFlush (BatchSize 0)`,
  `batchByOrFlush (BatchSize (-3))`, and
  `batchByOrFlushEither (BatchSize 0)` all raise an `ErrorCall`. Caught
  via `Control.Exception.try @ErrorCall`. (2026-04-17)
- `tasty-quickcheck 0.10` is not on Hackage; cabal solved 0.11.1 against
  the declared bound `>=0.10 && <0.12`. No action needed — the bound
  already spans both. (2026-04-17)
- `bytestring` turned out to be unnecessary in the test-suite
  `build-depends`: the pure tests never synthesise `ConsumerRecord`
  values (they work on `Either KafkaError Int` / `Either () Char`). Left
  it out to avoid declaring an unused dependency. (2026-04-17)


## Decision Log

- Decision: Use `tasty` + `tasty-hunit` + `tasty-quickcheck`.
  Rationale: tasty is the de facto framework in the modern Haskell ecosystem and integrates naturally with both unit and property testing. The alternative (hspec) is equally viable but tasty's tree-shaped output is easier to scan and its `--pattern` filter is convenient. No prior test convention exists in this repo, so either would work; picking tasty locks in a sensible default.
  Date: 2026-04-17

- Decision: Tests live in `hw-kafka-streamly/test/` alongside `src/`, per the normal Haskell convention.
  Rationale: keeps the cabal file simple and makes it obvious to a reader that tests belong to the library, not the jitsurei package.
  Date: 2026-04-17

- Decision: No Kafka broker integration tests in this plan. Those, if needed, belong in a separate future plan with a `process-compose` harness. EP-5 (jitsurei validation) already covers live broker testing through the cookbook examples.
  Rationale: scope — a pure test suite that runs on `cabal test` without dependencies is the valuable increment; broker tests add CI complexity that isn't needed for 0.1.0.0.
  Date: 2026-04-17

- Decision: Drop `bytestring` from the test-suite `build-depends` stanza
  that appeared in the original plan template.
  Rationale: the pure tests use `Int` / `Char` as the right-hand of the
  `Either` and never synthesise `ConsumerRecord (Maybe ByteString) (Maybe
  ByteString)` values, so `bytestring` is genuinely unused. The library's
  `common warnings` stanza does not enable `-Wunused-packages`, so an
  unused dep would not be flagged — still, better not to declare it.
  Date: 2026-04-17

- Decision: Use `Control.Monad.Catch`-throwable test exceptions (`TestErr
  = Good | Bad`) with `deriving stock (Eq, Show)` and a bare `instance
  Exception TestErr`, caught via `Control.Exception.try` at the `IO`
  level.
  Rationale: `throwLeft` / `throwLeftSatisfy` use `MonadThrow.throwM`.
  Instantiated at `IO`, `throwM = throwIO`, so the thrown value surfaces
  as a normal IO exception and is catchable by
  `Control.Exception.try`. `deriving stock` satisfies
  `-Wmissing-deriving-strategies`.
  Date: 2026-04-17

- Decision: Use `Positive Int` as the `Arbitrary` generator for
  `BatchSize` in QuickCheck properties rather than writing an orphan
  `Arbitrary BatchSize`.
  Rationale: avoids an orphan instance, and makes the positivity
  precondition (which the library itself enforces with `error`) visible
  at the property boundary.
  Date: 2026-04-17


## Outcomes & Retrospective

Implemented 2026-04-17. `cabal test hw-kafka-streamly` reports
`All 58 tests passed (0.00s)` with the following breakdown:

- Source / isFatal: 18 fatal-constructor cases + 3 non-fatal cases (21)
- Source / isPollTimeout: 4 cases
- Source / isPartitionEOF: 4 cases
- Source / skipNonFatal: 4 cases
- Source / skipNonFatalExcept: 3 cases
- Combinators / batchByOrFlush unit: 6 cases
- Combinators / batchByOrFlush property: 2 × 100-case QuickCheck runs
- Combinators / batchByOrFlushEither unit: 3 cases
- Combinators / batchByOrFlushEither property: 2 × 100-case QuickCheck runs
- Combinators / BatchSize validation: 3 cases
- Combinators / throwLeft: 3 cases
- Combinators / throwLeftSatisfy: 3 cases

`cabal check` continues to report "No errors or warnings could be found
in the package." `cabal build hw-kafka-streamly-test` produces no GHC
warnings under the `common warnings` flag set. Acceptance criteria
`N >= 40` met (58 tests).

Lesson: the plan anticipated a gap in the `isFatal` documentation (17
vs. 18 constructors) and the guard against non-positive `BatchSize` only
if EP-3 landed first. Both anticipations paid off — the full constructor
list was enumerable exhaustively, and the three validation tests were
easy to add unconditionally since EP-3 was already merged.


## Context and Orientation

The library lives at `/Users/shinzui/Keikaku/bokuno/hw-kafka-streamly/hw-kafka-streamly/`. Its cabal file currently has no `test-suite` stanza. The three source modules are:

- `src/Kafka/Streamly/Source.hs`
- `src/Kafka/Streamly/Sink.hs`
- `src/Kafka/Streamly/Combinators.hs`

### What the pure functions do, as of 2026-04-17

Pure functions that this plan will test:

**`Kafka.Streamly.Source`**:

    isFatal        :: KafkaError -> Bool
    isPollTimeout  :: KafkaError -> Bool
    isPartitionEOF :: KafkaError -> Bool

    skipNonFatal        :: Monad m => Stream m (Either KafkaError b) -> Stream m (Either KafkaError b)
    skipNonFatalExcept  :: Monad m => [KafkaError -> Bool] -> Stream m (Either KafkaError b) -> Stream m (Either KafkaError b)

`isFatal` pattern-matches on 16 specific `KafkaError` constructors (most of them `KafkaResponseError` wrapping a specific `RdKafkaRespErrT` variant) and returns `True` for those, `False` otherwise. Exhaustively testing every variant is feasible.

`isPollTimeout` is `e == KafkaResponseError RdKafkaRespErrTimedOut`.

`isPartitionEOF` is `e == KafkaResponseError RdKafkaRespErrPartitionEof`.

`skipNonFatal` filters a stream, keeping elements where `either isFatal (const True)` holds — i.e., drops `Left err` when `isFatal err` is false, keeps `Left err` when fatal, keeps all `Right`.

`skipNonFatalExcept fs` is the same but with an extended predicate list: keeps `Left err` when `isFatal err || any ($ err) fs`.

**`Kafka.Streamly.Combinators`**:

    batchByOrFlush       :: Monad m => BatchSize -> Stream m (Maybe a)   -> Stream m [a]
    batchByOrFlushEither :: Monad m => BatchSize -> Stream m (Either e a) -> Stream m [a]

    throwLeft         :: (MonadThrow m, Exception e) => Stream m (Either e a) -> Stream m a
    throwLeftSatisfy  :: (MonadThrow m, Exception e) => (e -> Bool) -> Stream m (Either e a) -> Stream m (Either e a)

`batchByOrFlush n xs` accumulates `Just a` elements into a batch of size `n`; on `Nothing` or stream end, flushes the current partial batch (empty batches are not emitted). `batchByOrFlushEither` is the same with `Left _` as the flush signal and `Right a` as the element signal.

`throwLeft` throws any `Left e` via `throwM` and passes `Right a` through as `a`.

`throwLeftSatisfy p` throws only `Left e` where `p e` is true; other `Left`s and all `Right`s pass through unchanged.

Note: EP-3 (API tightening) may add a validation guard in `batchInternal` that `error`s on non-positive `BatchSize`. If EP-3 lands first, tests for that behavior belong in this suite.

### Running streamly streams in tests

Streamly streams are parameterized by a monad. For pure tests, run them in `IO`:

    import qualified Streamly.Data.Stream as Stream
    import qualified Streamly.Data.Fold as Fold
    Stream.fold Fold.toList (streamTransformation (Stream.fromList input)) :: IO [a]

For property tests on pure combinators that do not need IO, `Stream.fold` generalizes to any `Monad m`. If the code under test has `MonadIO`/`MonadCatch` constraints, pick `IO` as the instantiation. For `throwLeft` tests, use `Control.Exception.try` to catch the thrown exception and assert on it.

### Test dependencies

Need to add to the `test-suite` stanza:

- `base`
- `bytestring` (for synthesizing `ConsumerRecord` values)
- `hw-kafka-client` (for the `KafkaError`/`RdKafkaRespErrT` types)
- `hw-kafka-streamly` (the library under test)
- `streamly-core` (to run streams)
- `tasty`
- `tasty-hunit`
- `tasty-quickcheck`


## Plan of Work

Four edits plus one new directory tree.


### 1. Add a `test-suite` stanza to the cabal file

File: `hw-kafka-streamly/hw-kafka-streamly.cabal`.

Append after the existing `library` stanza:

    test-suite hw-kafka-streamly-test
      import:             warnings
      type:               exitcode-stdio-1.0
      main-is:            Main.hs
      hs-source-dirs:     test
      other-modules:
        Kafka.Streamly.SourceTest
        Kafka.Streamly.CombinatorsTest
      build-depends:
        , base               >=4.21 && <5
        , bytestring         >=0.11 && <0.13
        , hw-kafka-client    >=5.3  && <6
        , hw-kafka-streamly
        , streamly-core      >=0.4  && <0.5
        , tasty              >=1.4  && <2
        , tasty-hunit        >=0.10 && <0.11
        , tasty-quickcheck   >=0.10 && <0.12
      default-language:   GHC2024
      default-extensions:
        DataKinds
        DuplicateRecordFields
        ImportQualifiedPost
        LambdaCase
        NoFieldSelectors
        OverloadedRecordDot
        OverloadedStrings
      ghc-options:        -threaded -rtsopts -with-rtsopts=-N


### 2. Create `hw-kafka-streamly/test/Main.hs`

Contents:

    module Main (main) where

    import qualified Kafka.Streamly.CombinatorsTest as CombinatorsTest
    import qualified Kafka.Streamly.SourceTest      as SourceTest
    import Test.Tasty (TestTree, defaultMain, testGroup)

    main :: IO ()
    main = defaultMain tests

    tests :: TestTree
    tests = testGroup "hw-kafka-streamly"
        [ SourceTest.tests
        , CombinatorsTest.tests
        ]


### 3. Create `hw-kafka-streamly/test/Kafka/Streamly/SourceTest.hs`

Structure: two test groups, one for error predicates, one for error filters.

Predicates to cover:

- `isFatal` returns `True` for each of the 16 fatal constructors enumerated in `Source.hs:114-132`:
  - `KafkaUnknownConfigurationKey ""`
  - `KafkaInvalidConfigurationValue ""`
  - `KafkaBadConfiguration`
  - `KafkaBadSpecification ""`
  - `KafkaResponseError RdKafkaRespErrDestroy`
  - `KafkaResponseError RdKafkaRespErrFail`
  - `KafkaResponseError RdKafkaRespErrInvalidArg`
  - `KafkaResponseError RdKafkaRespErrSsl`
  - `KafkaResponseError RdKafkaRespErrUnknownProtocol`
  - `KafkaResponseError RdKafkaRespErrNotImplemented`
  - `KafkaResponseError RdKafkaRespErrAuthentication`
  - `KafkaResponseError RdKafkaRespErrInconsistentGroupProtocol`
  - `KafkaResponseError RdKafkaRespErrTopicAuthorizationFailed`
  - `KafkaResponseError RdKafkaRespErrGroupAuthorizationFailed`
  - `KafkaResponseError RdKafkaRespErrClusterAuthorizationFailed`
  - `KafkaResponseError RdKafkaRespErrUnsupportedSaslMechanism`
  - `KafkaResponseError RdKafkaRespErrIllegalSaslState`
  - `KafkaResponseError RdKafkaRespErrUnsupportedVersion`

  and `False` for at least three non-fatal ones (`RdKafkaRespErrTimedOut`, `RdKafkaRespErrPartitionEof`, `RdKafkaRespErrNoError`).

- `isPollTimeout` returns `True` only for `KafkaResponseError RdKafkaRespErrTimedOut`; `False` for several others.

- `isPartitionEOF` returns `True` only for `KafkaResponseError RdKafkaRespErrPartitionEof`; `False` for several others.

Filters to cover:

- `skipNonFatal`: given a synthetic stream `[Right 1, Left (ResponseError Timedout), Right 2, Left (ResponseError Authentication), Right 3]`, the output is `[Right 1, Right 2, Left (ResponseError Authentication), Right 3]`. (Authentication is fatal; Timedout is not.)
- `skipNonFatalExcept [isPollTimeout]`: same input, output is `[Right 1, Left (ResponseError Timedout), Right 2, Left (ResponseError Authentication), Right 3]` (keeps timeouts).
- `skipNonFatal` passes all `Right` through unchanged.
- `skipNonFatal` drops all non-fatal `Left` even when `[]` is the extension list.

Helper to run a stream transformation to a list:

    runStream :: (Stream IO a -> Stream IO b) -> [a] -> IO [b]
    runStream f xs = Stream.fold Fold.toList (f (Stream.fromList xs))

Test body example:

    testSkipNonFatal :: TestTree
    testSkipNonFatal = testCase "skipNonFatal drops poll-timeout, keeps authentication" $ do
        let input =
                [ Right (1 :: Int)
                , Left (KafkaResponseError RdKafkaRespErrTimedOut)
                , Right 2
                , Left (KafkaResponseError RdKafkaRespErrAuthentication)
                , Right 3
                ]
        result <- runStream skipNonFatal input
        result @?= [Right 1, Right 2, Left (KafkaResponseError RdKafkaRespErrAuthentication), Right 3]


### 4. Create `hw-kafka-streamly/test/Kafka/Streamly/CombinatorsTest.hs`

Three test groups: batching (unit + property), throw-on-error (unit).

Batching unit cases:

- `batchByOrFlush (BatchSize 3) [Just 1, Just 2, Just 3, Just 4, Just 5] == [[1,2,3], [4,5]]`
- `batchByOrFlush (BatchSize 3) [Just 1, Nothing, Just 2, Just 3] == [[1], [2,3]]`
- `batchByOrFlush (BatchSize 3) [] == []`
- `batchByOrFlush (BatchSize 3) [Nothing, Nothing, Nothing] == []` (empty batches not emitted)
- `batchByOrFlush (BatchSize 2) [Just 1, Just 2, Just 3] == [[1,2], [3]]` (exact fill then trailing partial)
- `batchByOrFlushEither (BatchSize 2) [Right 'a', Right 'b', Left (), Right 'c'] == ["ab", "c"]`
- If EP-3 lands: `batchByOrFlush (BatchSize 0) ...` throws. Skip this if EP-3 hasn't landed yet; re-add later.

Batching properties (QuickCheck):

- For any `BatchSize n >= 1` and any list `xs :: [Maybe Int]`, `concat (batches n xs) == catMaybes xs`.
- For any `BatchSize n >= 1` and any `xs`, every emitted batch has `1 <= length <= n`.
- For `batchByOrFlushEither`, `concat (batches n xs) == rights xs`.

Throw-on-error unit cases:

- `throwLeft` on `[Right 1, Right 2, Right 3]` yields `[1,2,3]` and no exception.
- `throwLeft` on `[Right 1, Left SampleException, Right 3]` yields an exception on attempting to fold past index 1. Use `Control.Exception.try` and assert the exception constructor.
- `throwLeftSatisfy (== Bad)` on `[Right 1, Left Good, Right 2, Left Bad]` throws on `Left Bad`; `[Right 1, Left Good, Right 2]` came through first.

`SampleException`, `Good`, `Bad`: define a small sum type with `Exception` and `Eq` instances local to the test module.

Helper for batching:

    runBatchM :: BatchSize -> [Maybe Int] -> IO [[Int]]
    runBatchM n xs = Stream.fold Fold.toList (batchByOrFlush n (Stream.fromList xs))

    runBatchE :: BatchSize -> [Either () Char] -> IO [String]
    runBatchE n xs = Stream.fold Fold.toList (batchByOrFlushEither n (Stream.fromList xs))


## Concrete Steps

All commands run from the repository root `/Users/shinzui/Keikaku/bokuno/hw-kafka-streamly` unless noted.

Step 1. Create the test directory tree:

    mkdir -p hw-kafka-streamly/test/Kafka/Streamly

Step 2. Edit `hw-kafka-streamly/hw-kafka-streamly.cabal` per Plan of Work edit 1.

Step 3. Create `hw-kafka-streamly/test/Main.hs` per edit 2.

Step 4. Create `hw-kafka-streamly/test/Kafka/Streamly/SourceTest.hs` per edit 3.

Step 5. Create `hw-kafka-streamly/test/Kafka/Streamly/CombinatorsTest.hs` per edit 4.

Step 6. Build and run the tests:

    cabal test hw-kafka-streamly

Expected output (exact counts depend on implementation choices):

    Running 1 test suites...
    Test suite hw-kafka-streamly-test: RUNNING...
    hw-kafka-streamly
      Source
        isFatal
          all 16 fatal constructors:             OK
          non-fatal constructors:                OK
        isPollTimeout:                           OK
        isPartitionEOF:                          OK
        skipNonFatal:                            OK (3 cases)
        skipNonFatalExcept:                      OK (2 cases)
      Combinators
        batchByOrFlush unit:                     OK (5 cases)
        batchByOrFlush property:                 OK (100 tests)
        batchByOrFlushEither unit:               OK (1 case)
        batchByOrFlushEither property:           OK (100 tests)
        throwLeft unit:                          OK (2 cases)
        throwLeftSatisfy unit:                   OK (2 cases)

    All N tests passed (0.Ns)
    Test suite hw-kafka-streamly-test: PASS

Step 7. Verify cabal check still passes:

    cd hw-kafka-streamly && cabal check

Expected: `No errors or warnings could be found in the package.`

Step 8. Append to `hw-kafka-streamly/CHANGELOG.md` under `## 0.1.0.0 (unreleased)`:

    ### Tests

    - Initial pure test suite covering error predicates, filters, batching
      combinators, and exception combinators. Run with `cabal test`.

Step 9. Commit:

    git add hw-kafka-streamly/hw-kafka-streamly.cabal \
            hw-kafka-streamly/test/ \
            hw-kafka-streamly/CHANGELOG.md
    git commit -m "$(cat <<'EOF'
    Add pure test suite for hw-kafka-streamly

    Cover error predicates (isFatal, isPollTimeout, isPartitionEOF),
    error filters (skipNonFatal, skipNonFatalExcept), batching combinators
    (batchByOrFlush, batchByOrFlushEither), and exception combinators
    (throwLeft, throwLeftSatisfy). Uses tasty + tasty-hunit + tasty-quickcheck.
    No Kafka broker required.

    MasterPlan: docs/masterplans/1-release-hw-kafka-streamly-0.1.0.0.md
    ExecPlan: docs/plans/4-pure-test-suite.md
    Intention: intention_01knbcpkxqemdaawn3zzs822f8
    EOF
    )"


## Validation and Acceptance

Acceptance is:

1. `cabal test hw-kafka-streamly` produces `All N tests passed` with `N >= 40`.

2. No test is marked `SKIP` or `EXPECTED FAIL` in the output.

3. `cabal test hw-kafka-streamly --test-option=-l` lists at least: `isFatal`, `isPollTimeout`, `isPartitionEOF`, `skipNonFatal`, `skipNonFatalExcept`, `batchByOrFlush`, `batchByOrFlushEither`, `throwLeft`, `throwLeftSatisfy`.

4. `cd hw-kafka-streamly && cabal check` continues to pass.

5. `cabal build all` succeeds with no warnings after the new stanza is added.

6. The three test files compile with the project-wide `-Wall -Wcompat ...` flags without warnings (the `common warnings` import covers this).


## Idempotence and Recovery

Test files can be added, modified, and rerun freely. If a test fails, fix the test or the production code (coordinate with EP-3 via the Decision Log if production-code changes are needed).

If the `tasty` version bounds in the cabal file don't solve against the current package index, relax them — the three `tasty-*` packages are extremely stable in their APIs so minor version bumps are safe.


## Interfaces and Dependencies

New external dependencies in the test stanza of `hw-kafka-streamly/hw-kafka-streamly.cabal`:

- `tasty >= 1.4 && < 2`
- `tasty-hunit >= 0.10 && < 0.11`
- `tasty-quickcheck >= 0.10 && < 0.12`

No library code is edited by this plan. The library remains at the same API surface. The test suite depends on `hw-kafka-streamly` the same way any external consumer would — importing the exported modules and calling the exported functions.

Signatures that must exist in the test module layout:

    -- hw-kafka-streamly/test/Main.hs
    main :: IO ()

    -- hw-kafka-streamly/test/Kafka/Streamly/SourceTest.hs
    tests :: TestTree

    -- hw-kafka-streamly/test/Kafka/Streamly/CombinatorsTest.hs
    tests :: TestTree
