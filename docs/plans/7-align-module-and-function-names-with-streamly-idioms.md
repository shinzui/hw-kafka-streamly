---
id: 7
slug: align-module-and-function-names-with-streamly-idioms
title: "Align module and function names with streamly idioms"
kind: exec-plan
created_at: 2026-05-08T01:57:48Z
intention: "intention_01knbcpkxqemdaawn3zzs822f8"
---

# Align module and function names with streamly idioms

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

After this plan is complete, every public name in `hw-kafka-streamly` reflects the streaming abstraction it actually returns, using the same vocabulary `streamly` itself uses. A reader who already knows `streamly` will recognise every module and function name on sight; a reader new to both libraries will not have to learn a second, conduit-derived spelling for ideas that streamly already names. Specifically: `Kafka.Streamly.Source` becomes `Kafka.Streamly.Stream` (because every function in it returns `Streamly.Data.Stream.Stream`), and `Kafka.Streamly.Sink` becomes `Kafka.Streamly.Fold` (because every function in it returns `Streamly.Data.Fold.Fold`). The function families rename in lockstep — `kafkaSource` / `kafkaSourceAutoClose` / `kafkaSourceNoClose` become `kafkaStream` / `kafkaStreamAutoClose` / `kafkaStreamNoClose`; `kafkaSink` and `kafkaBatchSink` become `kafkaFold` and `kafkaBatchFold`.

A user can demonstrate the result by reading `Kafka.Streamly.Stream` in Haddock and seeing only stream-producing constructors that return `Stream m a`, and by reading `Kafka.Streamly.Fold` and seeing only fold-shaped consumers that return `Fold m a b`. They can run `cabal repl hw-kafka-streamly` and type `:t kafkaStream` to confirm the function exists with its `Stream m _` return type, and `:t kafkaFold` to confirm the same for `Fold m _ _`. They can run every cookbook executable in `hw-kafka-streamly-jitsurei` (`streamly-producer`, `streamly-consumer`, `error-handling`, `transform-pipeline`, `batch-fold`, `consume-produce`, `concurrent-consume`) under the new names and see the same observable behaviour as before — message counts, error filtering, end-to-end pipeline output. They can run `cabal test hw-kafka-streamly` and see the renamed test module `Kafka.Streamly.StreamTest` produce the same green test report.

The change is breaking. The library is published as `0.1.0.0`; this plan releases `0.2.0.0` with no backward-compatible re-exports, because adoption of `0.1.0.0` is minimal (the package was uploaded as a Hackage candidate days ago) and a clean break is cheaper to maintain than a deprecation surface that nobody is depending on. The CHANGELOG records the exhaustive name map so any 0.1.0.0 user can mechanically port their code.


## Progress

- [x] Milestone 1: Library rename — modules, functions, exports, internal references — 2026-05-08
  - [x] Renamed `hw-kafka-streamly/src/Kafka/Streamly/Source.hs` to `Stream.hs` (via `git mv`); module header now `module Kafka.Streamly.Stream`.
  - [x] Renamed functions `kafkaSource`, `kafkaSourceAutoClose`, `kafkaSourceNoClose` to `kafkaStream`, `kafkaStreamAutoClose`, `kafkaStreamNoClose`.
  - [x] Updated module-level Haddock and worked example imports to `Kafka.Streamly.Stream` / `kafkaStream`.
  - [x] Renamed `hw-kafka-streamly/src/Kafka/Streamly/Sink.hs` to `Fold.hs`; module header now `module Kafka.Streamly.Fold`.
  - [x] Renamed `kafkaSink`, `kafkaBatchSink` to `kafkaFold`, `kafkaBatchFold`.
  - [x] Updated module-level Haddock, worked example imports, and the cross-reference in `kafkaBatchFold`'s docstring.
  - [x] Edited `Kafka/Streamly/Combinators.hs`: docstring reference `'Kafka.Streamly.Source'` → `'Kafka.Streamly.Stream'`.
  - [x] Updated `hw-kafka-streamly.cabal` `exposed-modules:` to list `Kafka.Streamly.Fold` and `Kafka.Streamly.Stream`.
  - [x] Bumped cabal `version: 0.1.0.0` → `0.2.0.0`; rephrased `description:` from "stream sources / fold-based sinks" to "streams / folds".
  - [x] `cabal build hw-kafka-streamly` succeeds.
- [x] Milestone 2: Test suite rename — 2026-05-08
  - [x] Moved `SourceTest.hs` → `StreamTest.hs` (via `git mv`); module renamed to `Kafka.Streamly.StreamTest`.
  - [x] Updated `test/Main.hs` qualified import and call site.
  - [x] Updated `testGroup "Source"` → `testGroup "Stream"`; import switched to `Kafka.Streamly.Stream`.
  - [x] Updated cabal `other-modules:` from `Kafka.Streamly.SourceTest` to `Kafka.Streamly.StreamTest`.
  - [x] `cabal test hw-kafka-streamly`: all 58 tests pass.
- [x] Milestone 3: Cookbook rename — 2026-05-08
  - [x] Updated imports/call sites in `StreamlyConsumer.hs`, `StreamlyProducer.hs`, `ErrorHandling.hs`, `TransformPipeline.hs`, `BatchFold.hs`, `ConsumeProduce.hs`, `ConcurrentConsume.hs`, `Main.hs`.
  - [x] Renamed `app/BatchSink.hs` → `app/BatchFold.hs` (via `git mv`); module header is `module Main` already, no change needed there. Banner string and call site updated.
  - [x] Updated jitsurei cabal: `executable batch-sink` → `executable batch-fold`; `main-is: BatchSink.hs` → `BatchFold.hs`. Bumped jitsurei `version: 0.1.0.0` → `0.2.0.0`.
  - [x] Renamed local `source` → `stream` in `ConsumeProduce.hs` and `ConcurrentConsume.hs`.
  - [x] Updated help text in `Main.hs` to reflect the new exec name (`batch-fold`) and function names (`kafkaStream`, `kafkaFold`).
  - [x] `cabal build all` succeeds; all eight executables (including the new `batch-fold`) link cleanly.
- [x] Milestone 4: Documentation rename — 2026-05-08
  - [x] Updated `hw-kafka-streamly/README.md`: module bullets, Consuming/Producing prose and code blocks, build-depends bound bumped from `>=0.1 && <0.2` to `>=0.2 && <0.3`.
  - [x] Updated repo-root `README.md`: "batching sinks" → "batching folds" in the jitsurei layout description.
  - [x] Appended `## 0.2.0.0` section to `hw-kafka-streamly/CHANGELOG.md` with the exhaustive name-map table.
  - [x] Updated `mori.dhall` description: "stream sources / fold-based sinks" → "streams / folds" (and re-formatted via `dhall format`).
- [x] Milestone 5: Validation — 2026-05-08
  - [x] `cabal build all` succeeds with no warnings new to this plan.
  - [x] `cabal test hw-kafka-streamly`: 58/58 tests passing.
  - [x] `cabal haddock hw-kafka-streamly` succeeds. Pre-existing upstream warnings (KafkaError ambiguity, BatchSize link) remain — both documented in plan 3.
  - [x] Grep for legacy names returns only intentional retentions: the new 0.2.0.0 CHANGELOG name-map table, the historical 0.1.0.0 CHANGELOG entries, and the deliberate `@hw-kafka-conduit@'s @kafkaSource@` reference in `kafkaStream`'s docstring (a fact about a different package).
  - [x] `cabal repl hw-kafka-streamly` confirms `Kafka.Streamly.Stream.kafkaStream` returns `Stream m _` and `Kafka.Streamly.Fold.kafkaFold` returns `Fold m _ _`.
  - [x] `cabal run exe:hw-kafka-streamly-jitsurei` lists `batch-fold` (not `batch-sink`) and references `kafkaStream` / `kafkaFold` in the help text.
  - [x] Live broker roundtrip: `streamly-producer` (banner "via kafkaFold...") sent 5 records, `streamly-consumer` (banner "via kafkaStream...") read all 5 back. The redpanda cluster was already running from a pre-existing user-owned process-compose — `just process-down` reported a missing socket but with `|| true` exits cleanly; the cluster remains as the user had it.
- [ ] Milestone 6: Commit and tag
  - [ ] Single commit (or a small chain) with the Conventional Commits message described in the Concrete Steps section, including the `ExecPlan:` and `Intention:` git trailers.


## Surprises & Discoveries

- 2026-05-08 — `just process-up` attached to a pre-existing Redpanda cluster instead of starting a fresh one (the user already had `process-compose` running from a previous session). The roundtrip still worked because the cluster was healthy. `just process-down` failed to connect to its expected socket (the recipe uses `|| true` so it exited cleanly anyway). No action needed: the user's long-running broker is left alone.
- 2026-05-08 — `dhall format --check` failed on the existing `mori.dhall` (the file had not been formatted). Ran `dhall format mori.dhall` to canonicalise it; the only semantic change was the description rewrite I had already made.


## Decision Log

- Decision: Use `Kafka.Streamly.Stream` and `Kafka.Streamly.Fold` for the module names; rename functions to `kafkaStream*` and `kafkaFold*`.
  Rationale: Streamly's public-API vocabulary is unambiguous on this point. The package documentation file `streamly-project/docs/choosing_streamly_types.md` (read locally via `mori`) defines `Stream` as "produce sequences of values" and `Fold` as "consume sequences of values" — the exact two roles `Kafka.Streamly.Source` and `Kafka.Streamly.Sink` play. The streamly package itself ships modules named `Streamly.Data.Stream` and `Streamly.Data.Fold` at `streamly-project/streamly/core/src/Streamly/Data/Stream.hs` and `Fold.hs`. There is no public `Source` or `Sink` type in streamly's surface; "Source" appears only as `Streamly.Internal.Data.Producer.Source`, an internal buffer-pushback type unrelated to stream production. Using `Stream` / `Fold` makes the module names point a streamly user at the type they will actually receive. Two alternatives were considered and rejected: (a) `Consumer` / `Producer` (Kafka roles) was rejected because it inverts the streamly perspective — a Kafka consumer *produces* a stream of records, which is confusing every time the reader switches contexts — and because the names collide with the imported types `KafkaConsumer` / `KafkaProducer` from `hw-kafka-client`; (b) a hybrid (Kafka-role module names with streamly-typed function names) was rejected for adding two vocabularies where one suffices.
  Date: 2026-05-08

- Decision: Make a clean break at version 0.2.0.0 with no backward-compatible re-exports.
  Rationale: `0.1.0.0` was uploaded to Hackage on 2026-04-17 and adoption is minimal (the project was a candidate, not a regular release; the user is the only known consumer). Maintaining `Kafka.Streamly.Source` and `.Sink` as `{-# DEPRECATED #-}` re-export shims would carry maintenance cost (two copies of the module-level Haddock to keep in sync, deprecation warnings polluting downstream builds) for users who almost certainly do not exist. The CHANGELOG entry for `0.2.0.0` will list the exhaustive `old → new` name map so any holdout from `0.1.0.0` can port mechanically.
  Date: 2026-05-08

- Decision: Rename the jitsurei file `app/BatchSink.hs` to `BatchFold.hs` and the cabal executable `batch-sink` to `batch-fold`.
  Rationale: The jitsurei package is the documentation surface a reader sees first when they want to learn how to use the library. Leaving `BatchSink.hs` named for the deleted concept would force every reader to mentally translate "sink → fold" while reading the cookbook, defeating the point of the rename. The cookbook also serves as the canonical demonstration of names: `cabal run batch-fold` should match `kafkaBatchFold` exactly. The rename is mechanical (one file, one cabal stanza) and adds no risk.
  Date: 2026-05-08

- Decision: Do not rename helpers that don't carry the `Source` / `Sink` vocabulary.
  Rationale: `mapFirst`, `mapValue`, `bimapValue`, `skipNonFatal`, `skipNonFatalExcept`, `isFatal`, `isPollTimeout`, `isPartitionEOF`, `withKafkaProducer`, `BatchSize`, `batchByOrFlush`, `batchByOrFlushEither`, `throwLeft`, `throwLeftSatisfy` are described in terms of what they do, not the conduit/streamly abstraction layer. They keep their names and need no churn.
  Date: 2026-05-08

- Decision: Keep `Kafka.Streamly.Combinators` as the module name for the cross-cutting combinators.
  Rationale: That module is neither stream-shaped nor fold-shaped — it contains both stream-to-stream functions (`throwLeft*`, `batchByOrFlush*`) and re-exports (`BatchSize`). Renaming it would have no naming-alignment payoff. Streamly itself uses `Combinators` in some module names (e.g., `Streamly.Internal.Data.Stream.Combinators`).
  Date: 2026-05-08


## Outcomes & Retrospective

The rename landed cleanly. Library and jitsurei build at 0.2.0.0; 58 tests pass; haddock builds with only the pre-existing upstream warnings; the live broker roundtrip exercises both renamed call sites (`kafkaFold` for production, `kafkaStream` for consumption).

What went well:

- Sequencing — library, then test suite, then cookbook — meant each milestone reached a buildable state on its own. The first `cabal build hw-kafka-streamly` after the library moves caught the kind of typo that's expensive to find later (in this case there were none).
- The grep-for-legacy-names step from the Validation milestone was decisive — it surfaced the three intentional retentions exactly and nothing else, which made it easy to certify the rename complete.
- Renaming the local `source` variables to `stream` in the cookbook (a small step the plan explicitly called out) keeps the cookbook reading consistent with the new library names; without it, a reader would still see the old vocabulary in the bindings even though the imports said otherwise.

Friction points:

- One Edit operation accidentally injected a duplicate `@since 0.1.0.0` block in `Fold.hs` after I matched on a multi-line `old_string` whose content was already present elsewhere. Caught immediately in the next read; fixed in a single follow-up Edit. The lesson: when an Edit adds content that already exists nearby, prefer reading the surrounding context first to confirm the anchor is unique.
- `mori.dhall` wasn't dhall-formatted before this change. The formatter added record-field-leading commas on every record literal, producing a larger diff than the description-only rewrite this plan called for. Acceptable — the formatter is canonical — but worth flagging to anyone reviewing the commit.

Next steps (not part of this plan): release 0.2.0.0 to Hackage. The CHANGELOG release-date placeholder is `<release date>` and is replaced as part of that work, mirroring how plan 6 did it for 0.1.0.0.


## Context and Orientation

This repository, `hw-kafka-streamly`, provides streaming bindings between two Haskell libraries: `hw-kafka-client` (Kafka via librdkafka) and `streamly` (a high-performance streaming library). The library was historically created by porting `hw-kafka-conduit` — the `conduit`-based bindings shipped in the `haskell-works/hw-kafka-client` repository — to streamly. The port preserved conduit's vocabulary verbatim: a stream-producing function was named `kafkaSource` and lived in `Kafka.Conduit.Source`, so it landed in this package as `kafkaSource` in `Kafka.Streamly.Source`. The same is true for `kafkaSink`. This is the misalignment the present plan addresses.

Every name relevant to this plan lives in two cabal packages inside the repository:

- `hw-kafka-streamly/` — the published library. Source under `hw-kafka-streamly/src/`, tests under `hw-kafka-streamly/test/`. The cabal file is `hw-kafka-streamly/hw-kafka-streamly.cabal`. The package's user-facing README is `hw-kafka-streamly/README.md` (this is the file that renders on Hackage). The CHANGELOG is `hw-kafka-streamly/CHANGELOG.md`.

- `hw-kafka-streamly-jitsurei/` — runnable cookbook examples. "Jitsurei" (実例) means "real example". The package is not published and its cabal file is `hw-kafka-streamly-jitsurei/hw-kafka-streamly-jitsurei.cabal`. The cookbook ships seven executables: `streamly-producer`, `streamly-consumer`, `error-handling`, `transform-pipeline`, `batch-sink` (to be renamed to `batch-fold`), `consume-produce`, `concurrent-consume`, plus a help-printing default executable `hw-kafka-streamly-jitsurei`.

The current public API is:

    module Kafka.Streamly.Source where
        kafkaSource          :: (MonadIO m, MonadCatch m)
                             => ConsumerProperties -> Subscription -> Timeout
                             -> Stream m (Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))
        kafkaSourceAutoClose :: (MonadIO m, MonadCatch m) => KafkaConsumer -> Timeout -> Stream m _
        kafkaSourceNoClose   :: MonadIO m => KafkaConsumer -> Timeout -> Stream m _
        isFatal, isPollTimeout, isPartitionEOF :: KafkaError -> Bool
        skipNonFatal, skipNonFatalExcept       :: ...
        mapFirst, mapValue, bimapValue         :: ...

    module Kafka.Streamly.Sink where
        kafkaSink         :: MonadIO m => KafkaProducer -> Fold m ProducerRecord (Maybe KafkaError)
        kafkaBatchSink    :: MonadIO m => KafkaProducer -> Fold m [ProducerRecord] (Maybe KafkaError)
        withKafkaProducer :: ProducerProperties -> (KafkaProducer -> IO a) -> IO (Either KafkaError a)

    module Kafka.Streamly.Combinators where
        BatchSize(..), batchByOrFlush, batchByOrFlushEither
        throwLeft, throwLeftSatisfy

After this plan, the public API is:

    module Kafka.Streamly.Stream where
        kafkaStream          :: (MonadIO m, MonadCatch m) => ConsumerProperties -> Subscription -> Timeout -> Stream m _
        kafkaStreamAutoClose :: (MonadIO m, MonadCatch m) => KafkaConsumer -> Timeout -> Stream m _
        kafkaStreamNoClose   :: MonadIO m => KafkaConsumer -> Timeout -> Stream m _
        -- (everything else unchanged)

    module Kafka.Streamly.Fold where
        kafkaFold         :: MonadIO m => KafkaProducer -> Fold m ProducerRecord (Maybe KafkaError)
        kafkaBatchFold    :: MonadIO m => KafkaProducer -> Fold m [ProducerRecord] (Maybe KafkaError)
        withKafkaProducer :: -- unchanged

    module Kafka.Streamly.Combinators where
        -- unchanged

The key terms in plain English. **Stream** is streamly's name for a sequence of values that something can pull from. It is the type returned by every `kafka*` function in `Kafka.Streamly.Source` (today) / `Kafka.Streamly.Stream` (after this plan). Concretely: `Stream m a` is "a stream of `a`s, computed in monad `m`." **Fold** is streamly's name for a thing that consumes a `Stream` and produces a single result; concretely `Fold m a b` is "consume `a`s in monad `m`, produce a `b`." **Source** and **Sink** are the corresponding terms from a different streaming library, `conduit`; streamly does not expose a `Source` or `Sink` type in its public API. **Conduit** is the streaming library that the original Haskell-works package `hw-kafka-conduit` was built on, and from which `hw-kafka-streamly` inherited its current names by direct port.

Why the names matter beyond aesthetics. Function and module names tell a reader where to look in the dependency for the type they are about to receive. A reader who imports `kafkaSource` and then types `:t kafkaSource` in `cabal repl` sees `Stream m _` and has to mentally bridge "Source produces a Stream — they're the same thing in different libraries' words." A reader who imports `kafkaStream` sees `Stream m _` and the bridge collapses: the function name and the type name coincide.

Naming history / where the conduit terms came from. The upstream `hw-kafka-conduit` lives at `/Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project/hw-kafka-conduit/src/Kafka/Conduit/{Source,Sink}.hs`. Inspecting that source file confirms: `Kafka.Conduit.Source` exports `kafkaSource`, `kafkaSourceNoClose`, `kafkaSourceAutoClose`; `Kafka.Conduit.Sink` exports `kafkaSink`, `kafkaSinkAutoClose`, `kafkaSinkNoClose`, `kafkaBatchSinkNoClose`. Our 0.1.0.0 module shape and function names mirror conduit's exactly, which was the right call during the port (it minimised cognitive load for migrating users) but is the wrong call long-term once the port is done.

Where streamly's naming canon lives. The streamly project ships a guide at `streamly-project/docs/choosing_streamly_types.md`, registered in mori under `mori://composewell/streamly/docs/choosing-streamly-types`. The relevant excerpt:

    | Type          | Purpose                              | Key Characteristic                                   |
    |---------------|--------------------------------------|------------------------------------------------------|
    | **Stream**    | Produce sequences of values          | High performance with fusion, static composition     |
    | **Fold**      | Consume sequences of values          | Efficient reduction, no backtracking                 |
    | **Unfold**    | Generate streams from seed values    | Efficient for nested loops and reusable generators   |
    | **Scanl**     | Transform streams with state         | Produces intermediate results at each step           |
    | **Parser**    | Parse structured data                | Stream consumer with backtracking                    |

`hw-kafka-streamly`'s consumer functions are **Stream** (`Stream.unfoldrM`-based pollers; pure stream producers) and its producer functions are **Fold** (`Fold.foldlM'`-based consumers that reduce to `Maybe KafkaError`). They are not `Unfold`s (which would produce a stream from a re-usable seed; our consumer holds a captured `KafkaConsumer` handle and is not seed-parameterised), nor `Scanl`s (no intermediate-result emission), nor `Parser`s (no backtracking). The two streamly types `Stream` and `Fold` are the exact, unique correct labels.

Surface area of the rename. The names `Source`, `Sink`, `kafkaSource*`, `kafkaSink*` appear in:

- 3 library source files: `hw-kafka-streamly/src/Kafka/Streamly/{Source,Sink,Combinators}.hs`.
- 2 test files: `hw-kafka-streamly/test/Main.hs`, `hw-kafka-streamly/test/Kafka/Streamly/SourceTest.hs`.
- 8 cookbook source files: `hw-kafka-streamly-jitsurei/app/{StreamlyConsumer,StreamlyProducer,ErrorHandling,TransformPipeline,BatchSink,ConsumeProduce,ConcurrentConsume,Main}.hs`.
- 2 cabal files: `hw-kafka-streamly/hw-kafka-streamly.cabal`, `hw-kafka-streamly-jitsurei/hw-kafka-streamly-jitsurei.cabal`.
- 2 README files: `hw-kafka-streamly/README.md` and the repo-root `README.md`.
- 1 CHANGELOG: `hw-kafka-streamly/CHANGELOG.md` — only the new `0.2.0.0` section is written; the historical `0.1.0.0` section keeps its (then-accurate) wording.
- Possibly `mori.dhall` if its `description` field mentions "sources" / "sinks". It does — the package `description` reads "composable stream sources for consuming and fold-based sinks for producing" — and will be reworded.
- Prior plan files under `docs/plans/1-*.md`, `2-*.md`, `3-*.md`, `4-*.md`, `5-*.md`, `6-*.md` and the MasterPlan under `docs/masterplans/`. These are historical records of work that targeted the 0.1.0.0 names; do not retroactively rewrite them. The current plan (this file, plan 7) is allowed and required to use the new names from the start; the MasterPlan is not amended retroactively.

Out of scope for this plan. (a) Any further surface change beyond renaming — Haddock prose stays as-is except where it references the renamed identifiers; behaviour stays identical. (b) Hackage upload of `0.2.0.0` — that is the next plan's job, mirroring how plan 6 handled the `0.1.0.0` upload. This plan ends with the local working tree at version `0.2.0.0` and a green test/build, but the sdist/upload step is deferred. (c) The conduit `Source`/`Sink` vocabulary anywhere in third-party packages or in `hw-kafka-conduit` itself; this plan only touches files inside this repository.


## Plan of Work

The work is naturally six milestones. Each leaves the working tree in a buildable state and is independently verifiable. The rename is mechanical — there are no semantic changes — so the sequencing principle is "rename in dependency order so each step compiles": the library first (its public API is what everything else imports), then the test suite (depends on the library), then the cookbook (depends on the library), then documentation (text-only, no compile dependency), then the validation, then the commit.

### Milestone 1: Library rename

Goal at end of milestone: the library package `hw-kafka-streamly` exposes `Kafka.Streamly.Stream` and `Kafka.Streamly.Fold` instead of `Kafka.Streamly.Source` and `Kafka.Streamly.Sink`, with all internal references updated. `cabal build hw-kafka-streamly` succeeds. The test suite and the cookbook do not yet compile — that is intentional and addressed in the next two milestones.

The library lives at `hw-kafka-streamly/src/Kafka/Streamly/`. Three files exist there: `Source.hs`, `Sink.hs`, `Combinators.hs`. The work:

Move `Source.hs` to `Stream.hs`. Inside the file, change the `Module` Haddock field from `Kafka.Streamly.Source` to `Kafka.Streamly.Stream`, and the `Description` field from "Consumer stream sources and helpers for Streamly–Kafka pipelines." to "Consumer streams and helpers for Streamly–Kafka pipelines." Change the `module Kafka.Streamly.Source ( … ) where` line to `module Kafka.Streamly.Stream ( … ) where`. Inside the export list and throughout the file body, rename the functions `kafkaSource`, `kafkaSourceAutoClose`, `kafkaSourceNoClose` to `kafkaStream`, `kafkaStreamAutoClose`, `kafkaStreamNoClose` — including every recursive call (`kafkaSourceAutoClose` calls `kafkaSourceNoClose`; `kafkaSource` calls `kafkaSourceNoClose`) and every `INLINE` pragma. Inside the module-level Haddock at the top of the file, update the bulleted comment block ("`'kafkaSource'` — fully managed: …") to use the new names, and update the worked-example transcript (which currently imports `Kafka.Streamly.Source`) to import `Kafka.Streamly.Stream` and call `kafkaStream`. Inside the docstring of `kafkaStream` (formerly `kafkaSource`), update the comparison sentence "This diverges from `hw-kafka-conduit`'s `kafkaSource`, which yields …" — keep the reference to the conduit name (it is correct in context, naming a separate package), but rephrase the rest to refer to `kafkaStream` and `kafkaStreamAutoClose`.

Move `Sink.hs` to `Fold.hs`. Update the `Module` field to `Kafka.Streamly.Fold` and the `Description` field to "Producer folds and bracket helper for Streamly–Kafka pipelines." Change the module declaration and rename the functions `kafkaSink` → `kafkaFold` and `kafkaBatchSink` → `kafkaBatchFold` throughout — definitions, type signatures, INLINE pragmas, the recursive `sendBatch` helper inside `kafkaBatchFold` is unchanged because it has no `Sink` in its name. Update the module-level docstring's reference to "`'Kafka.Streamly.Combinators.batchByOrFlush'`" — that is a reference to a different module and is not affected by this milestone, but the surrounding sentence "the output of …" is unchanged. Update the worked-example transcript so it imports `Kafka.Streamly.Fold` and calls `kafkaFold`.

Edit `hw-kafka-streamly/src/Kafka/Streamly/Combinators.hs`. The module-level Haddock currently says "Auxiliary combinators that bridge consumer streams and producer folds: …". That phrasing is already correct under the new vocabulary — no change. But further down (line 8 in the current file) the docstring references `'Kafka.Streamly.Source'` by name; rewrite that to `'Kafka.Streamly.Stream'`. Search the file body for any other `Source` / `Sink` references and rewrite.

Edit `hw-kafka-streamly/hw-kafka-streamly.cabal`. In the `library` stanza, replace the two lines

    Kafka.Streamly.Sink
    Kafka.Streamly.Source

with

    Kafka.Streamly.Fold
    Kafka.Streamly.Stream

(alphabetical, like the current ordering). Bump `version: 0.1.0.0` to `version: 0.2.0.0`. No other cabal field changes — `synopsis`, `description`, dependency bounds, etc. stay exactly as they are except the description field uses the words "sources" / "sinks" already and may be reworded to "streams" / "folds" for accuracy. Looking at the current `description` it says "Provides composable stream sources for consuming and fold-based sinks for producing" — rephrase to "Provides composable streams for consuming and folds for producing".

Run `cabal build hw-kafka-streamly`. Expected: success, with at most the same warnings that `0.1.0.0` produced (notably the upstream `streamly-0.12.0` `-Wunused-packages` warning documented in plan 3). The test suite and the jitsurei package will fail to build at this point because they still reference the old module names; that is acceptable — they are addressed in the next milestones. Use `cabal build hw-kafka-streamly` (not `cabal build all`) to avoid the false-positive failure during this milestone.

Acceptance: `cabal build hw-kafka-streamly` exits 0 with no new warnings. `grep -rE "kafkaSource|kafkaSink|Kafka\.Streamly\.Source|Kafka\.Streamly\.Sink" hw-kafka-streamly/src` returns nothing. `cabal repl hw-kafka-streamly` followed by `:t Kafka.Streamly.Stream.kafkaStream` and `:t Kafka.Streamly.Fold.kafkaFold` shows the expected types.

### Milestone 2: Test suite rename

Goal at end of milestone: the test module that exercised the renamed functions matches the new names; `cabal test hw-kafka-streamly` is green.

The test file at `hw-kafka-streamly/test/Kafka/Streamly/SourceTest.hs` contains a `module Kafka.Streamly.SourceTest (tests) where` declaration, an import of `Kafka.Streamly.Source`, and a top-level `testGroup` labelled `"Source"`. Move the file to `hw-kafka-streamly/test/Kafka/Streamly/StreamTest.hs`. Rename the module to `Kafka.Streamly.StreamTest`. Update the import to `Kafka.Streamly.Stream` and the `testGroup` label to `"Stream"`. Update every reference to `kafkaSource*` in the test bodies; the existing tests cover the error predicates and filters, which were not renamed, but they likely import functions like `isFatal`, `skipNonFatal`, etc. that survive unchanged. The grep step at the end will catch any missed reference.

Edit `hw-kafka-streamly/test/Main.hs`. Change the import `import Kafka.Streamly.SourceTest qualified as SourceTest` to `import Kafka.Streamly.StreamTest qualified as StreamTest` and the call-site `SourceTest.tests` to `StreamTest.tests`.

Edit `hw-kafka-streamly/hw-kafka-streamly.cabal` in the `test-suite hw-kafka-streamly-test` stanza. Under `other-modules:`, change `Kafka.Streamly.SourceTest` to `Kafka.Streamly.StreamTest`.

Acceptance: `cabal test hw-kafka-streamly` runs and reports the same number of tests passing as before this plan. The test group label `"Stream"` appears in the output where `"Source"` previously did.

### Milestone 3: Cookbook rename

Goal at end of milestone: every executable in `hw-kafka-streamly-jitsurei` builds against the renamed library. The cabal package builds cleanly. The renamed `batch-fold` executable exists.

Eight files in `hw-kafka-streamly-jitsurei/app/` need editing. For each, swap imports and call sites: `Kafka.Streamly.Source` → `Kafka.Streamly.Stream`, `Kafka.Streamly.Sink` → `Kafka.Streamly.Fold`, `kafkaSource` → `kafkaStream`, `kafkaSink` → `kafkaFold`, `kafkaBatchSink` → `kafkaBatchFold`. The files are: `StreamlyConsumer.hs`, `StreamlyProducer.hs`, `ErrorHandling.hs`, `TransformPipeline.hs`, `BatchSink.hs` (also rename to `BatchFold.hs`), `ConsumeProduce.hs`, `ConcurrentConsume.hs`, `Main.hs`. `Main.hs` does not import the library — it prints help text — but its help strings reference `kafkaSource`, `kafkaSink`, and the `batch-sink` executable name. Update those.

Inside `ConsumeProduce.hs` (line 68) and `ConcurrentConsume.hs` (line 60), local variables named `source` are bound to the result of `kafkaStream …`. Rename those bindings to `stream`. The variable rename is local and does not affect any external interface; it is included for vocabulary consistency inside the cookbook. There is no analogous local `sink` to worry about.

Move `hw-kafka-streamly-jitsurei/app/BatchSink.hs` to `hw-kafka-streamly-jitsurei/app/BatchFold.hs`. Inside, change the `module` line from `module Main` (executable cookbook modules conventionally name themselves `Main`; verify in the file) — no change to `module Main` itself, but check the imports and Haddock. Inside the module Haddock and in any `putStrLn` text, change references from `BatchSink` and `kafkaBatchSink` to `BatchFold` and `kafkaBatchFold`.

Edit `hw-kafka-streamly-jitsurei/hw-kafka-streamly-jitsurei.cabal`. The `executable batch-sink` stanza becomes `executable batch-fold`; its `main-is: BatchSink.hs` becomes `main-is: BatchFold.hs`. No other cabal change is needed — the other executables already point at unchanged file names. Bump the jitsurei package version to `0.2.0.0` for consistency with the library bump (the jitsurei package is unpublished but its version field should track the library it documents).

Run `cabal build all`. Expected: success.

Acceptance: `cabal build all` exits 0 with no new warnings. `cabal run --` (with no args) prints the help text from `Main.hs` and lists `batch-fold` (not `batch-sink`). Each executable name resolves: `cabal run streamly-producer --help` and `cabal run batch-fold --help` print at least the executable name and exit cleanly.

### Milestone 4: Documentation rename

Goal at end of milestone: every prose document outside of historical-plan files is internally consistent with the new names. A new CHANGELOG section explains the rename.

Edit `hw-kafka-streamly/README.md`. Search for every occurrence of `Kafka.Streamly.Source`, `Kafka.Streamly.Sink`, `kafkaSource`, `kafkaSink`, and rewrite. The README has bulleted module summaries ("`Kafka.Streamly.Source` — consumer streams, error predicates and filters, …"), worked-example imports (`import Kafka.Streamly.Source (kafkaSource, skipNonFatal)`), and prose paragraphs ("The source module ships three variants …"). Rephrase the prose paragraph to "The stream module ships three variants …". The rest of the rewrite is mechanical.

Inspect repo-root `README.md`. Its current text mentions only the abstraction-level "`Stream`s and `Fold`s" — verify that no occurrence of `Source` or `Sink` remains. (My research confirms the root README does not reference the deprecated module names directly except in the second-paragraph phrase "Kafka consumers become Streamly `Stream`s and producers become Streamly `Fold`s", which is already aligned with the new names.) If new uses appear, edit them.

Append a new top section to `hw-kafka-streamly/CHANGELOG.md`:

    ## 0.2.0.0 — <release date>

    ### Changed (BREAKING)

    - Renamed modules and functions to use streamly's vocabulary instead of
      conduit's. The functions return Streamly `Stream`s and `Fold`s, so the
      modules are now named `Kafka.Streamly.Stream` and `Kafka.Streamly.Fold`,
      and the function families are `kafkaStream*` and `kafkaFold*`.

      | Old (0.1.0.0)                  | New (0.2.0.0)                |
      |--------------------------------|------------------------------|
      | `Kafka.Streamly.Source`        | `Kafka.Streamly.Stream`      |
      | `Kafka.Streamly.Sink`          | `Kafka.Streamly.Fold`        |
      | `kafkaSource`                  | `kafkaStream`                |
      | `kafkaSourceAutoClose`         | `kafkaStreamAutoClose`       |
      | `kafkaSourceNoClose`           | `kafkaStreamNoClose`         |
      | `kafkaSink`                    | `kafkaFold`                  |
      | `kafkaBatchSink`               | `kafkaBatchFold`             |

      No backward-compatible re-exports are provided. Callers porting from
      0.1.0.0 should mechanically substitute identifiers per the table.

The release-date placeholder stays as `<release date>` until the upload plan replaces it; that mirrors the editing convention plan 6 used for `0.1.0.0`.

Edit `mori.dhall`. Today the package `description` reads "Streamly streaming bindings for hw-kafka-client — composable stream sources for consuming and fold-based sinks for producing Kafka messages". Rewrite to "Streamly streaming bindings for hw-kafka-client — composable streams for consuming and folds for producing Kafka messages". Run `mori show --full` afterwards and confirm the new description prints.

Acceptance: `grep -rE "kafkaSource|kafkaSink|Kafka\.Streamly\.Source|Kafka\.Streamly\.Sink|kafkaBatchSink" hw-kafka-streamly hw-kafka-streamly-jitsurei mori.dhall README.md` returns nothing in source/cabal/README/CHANGELOG/dhall files; `git ls-files --modified` includes the expected files; `mori show --full` prints the rewritten description.

### Milestone 5: Validation

Goal at end of milestone: the renamed working tree builds, tests, and runs end-to-end against a real broker.

Run, in order, from the repository root:

    cabal build all                               # expect: success
    cabal test hw-kafka-streamly                  # expect: green
    cabal haddock hw-kafka-streamly               # expect: success, no new warnings

The Haddock step inherits two upstream-environment warnings documented in plan 3: the `KafkaError` / `BatchSize` ambiguity warnings from `hw-kafka-client`, and the missing-Haddock-on-`streamly-core` link warnings. Neither is introduced by this plan; both must remain pre-existing.

For the live cookbook check, start a Redpanda broker via the checked-in `process-compose.yaml` and run a roundtrip:

    just process-up
    just create-topic
    cabal run streamly-producer
    cabal run streamly-consumer
    just process-down

Expected output for `streamly-producer`: prints "Producing 5 messages to TopicName \"jitsurei-topic\" via kafkaFold..." (note: the help/banner string in `StreamlyProducer.hs` mentions `kafkaSink` today; that is renamed in milestone 3) followed by 5 record sends and "Producer flushed and closed." Expected output for `streamly-consumer`: prints "Consuming up to 10 messages from TopicName \"jitsurei-topic\" via kafkaStream..." followed by up to 10 received records.

If you cannot run a Redpanda broker (e.g. no Docker available), record this constraint in the Surprises & Discoveries section and skip the live step. The cabal build + test + haddock steps remain mandatory.

Acceptance: all three cabal commands exit 0; the cookbook end-to-end run prints expected output; an exhaustive grep for the legacy names

    grep -rE "kafkaSource|kafkaSink|Kafka\.Streamly\.Source|Kafka\.Streamly\.Sink|kafkaBatchSink" \
        hw-kafka-streamly hw-kafka-streamly-jitsurei mori.dhall README.md \
        | grep -v "docs/plans/[1-6]-" \
        | grep -v "docs/masterplans/"

returns nothing.

### Milestone 6: Commit and tag

Goal at end of milestone: the rename lands as a single Conventional Commits commit (or a small chain of commits broken by milestone) on the current branch with the required git trailers. No release tag is created in this plan — that belongs to the future Hackage-release plan.

Stage the changed files with `git add`. Build the commit message via heredoc (per the agent commit rules) with this content:

    refactor!: rename Source/Sink to Stream/Fold to match streamly idioms

    The library exports streamly Streams and Folds, but its modules and
    functions used the conduit-derived names Source and Sink. Rename so
    that names match the types the functions actually return:

      Kafka.Streamly.Source -> Kafka.Streamly.Stream
      Kafka.Streamly.Sink   -> Kafka.Streamly.Fold
      kafkaSource*          -> kafkaStream*
      kafkaSink             -> kafkaFold
      kafkaBatchSink        -> kafkaBatchFold

    Cookbook follow-ups: BatchSink.hs renamed to BatchFold.hs, executable
    batch-sink renamed to batch-fold, local variables named source renamed
    to stream.

    No backward-compatible re-exports are provided; this is a breaking
    change at version 0.2.0.0.

    ExecPlan: docs/plans/7-align-module-and-function-names-with-streamly-idioms.md
    Intention: intention_01knbcpkxqemdaawn3zzs822f8

The `refactor!:` prefix marks a breaking change per Conventional Commits. The trailer block at the bottom is required by the repository's plan workflow (`ExecPlan:`) and intention workflow (`Intention:`).

Acceptance: `git log -1 --format=fuller` shows the commit with both trailers; `git diff HEAD~1 -- hw-kafka-streamly hw-kafka-streamly-jitsurei` shows the expected file moves and content edits; `git status` is clean.


## Concrete Steps

All commands assume the working directory is the repository root unless stated otherwise.

Inspect the current state.

    git status
    git log --oneline -5
    grep -rcE "kafkaSource|kafkaSink|Kafka\.Streamly\.Source|Kafka\.Streamly\.Sink|kafkaBatchSink" \
        hw-kafka-streamly hw-kafka-streamly-jitsurei mori.dhall README.md

Expect a baseline count similar to the research run that produced this plan: ~70 occurrences across ~15 files.

Milestone 1 file moves.

    git mv hw-kafka-streamly/src/Kafka/Streamly/Source.hs hw-kafka-streamly/src/Kafka/Streamly/Stream.hs
    git mv hw-kafka-streamly/src/Kafka/Streamly/Sink.hs   hw-kafka-streamly/src/Kafka/Streamly/Fold.hs

Then edit the moved files in place using the Edit tool. The exact edits are described in the Plan of Work; in summary, change the module declaration, rename the function families, and rewrite the Haddock module-level docstrings and worked-example imports.

Edit the cabal file:

    # in hw-kafka-streamly/hw-kafka-streamly.cabal:
    #   exposed-modules: replace Sink/Source with Fold/Stream
    #   version: bump 0.1.0.0 -> 0.2.0.0
    #   description: rephrase "stream sources … sinks" -> "streams … folds"

Verify the library builds:

    cabal build hw-kafka-streamly

Expected output ends with:

    [3 of 3] Compiling Kafka.Streamly.Combinators ( ... )
    [4 of 4] Compiling Kafka.Streamly.Fold        ( ... )
    [5 of 5] Compiling Kafka.Streamly.Stream      ( ... )
    Linking ...

Milestone 2:

    git mv hw-kafka-streamly/test/Kafka/Streamly/SourceTest.hs hw-kafka-streamly/test/Kafka/Streamly/StreamTest.hs

Edit the moved test file (rename module, update imports, update group label) and `hw-kafka-streamly/test/Main.hs` (update `qualified as` import) and the cabal `other-modules:` entry. Then:

    cabal test hw-kafka-streamly

Expected: a `tasty` summary line "All N tests passed" where N matches the count from the 0.1.0.0 baseline.

Milestone 3:

    git mv hw-kafka-streamly-jitsurei/app/BatchSink.hs hw-kafka-streamly-jitsurei/app/BatchFold.hs

Edit the eight cookbook source files and the jitsurei cabal file as described above. Then:

    cabal build all

Expected: a build of the library, the test suite, and all eight executables. The new executable name `batch-fold` appears in the build log; `batch-sink` does not.

Milestone 4:

Edit `hw-kafka-streamly/README.md`, append the `0.2.0.0` section to `hw-kafka-streamly/CHANGELOG.md`, and edit the description in `mori.dhall`. Then:

    mori show --full | head -20

Expected output: the package `Description:` line reflects the new "streams … folds" wording.

Milestone 5:

    cabal build all
    cabal test hw-kafka-streamly
    cabal haddock hw-kafka-streamly
    grep -rE "kafkaSource|kafkaSink|Kafka\.Streamly\.Source|Kafka\.Streamly\.Sink|kafkaBatchSink" \
        hw-kafka-streamly hw-kafka-streamly-jitsurei mori.dhall README.md \
        | grep -v "docs/plans/[1-6]-" \
        | grep -v "docs/masterplans/"

Expected: the three cabal commands exit 0 with the warnings documented above; the grep returns no matches.

Optional live broker check:

    just process-up
    just create-topic
    cabal run streamly-producer
    cabal run streamly-consumer
    just process-down

Expected output: see the Validation milestone section.

Milestone 6:

    git add hw-kafka-streamly hw-kafka-streamly-jitsurei mori.dhall docs/plans/7-*.md
    git status

Then commit with the heredoc message shown in the Plan of Work / Milestone 6 section. After commit:

    git log -1 --format=fuller

Expected: the commit lists both `ExecPlan:` and `Intention:` trailers and the `refactor!:` subject.


## Validation and Acceptance

The plan succeeds when, simultaneously:

1. `cabal build all` exits 0 with no new warnings beyond those documented in plan 3.
2. `cabal test hw-kafka-streamly` is green and reports the same test count as the 0.1.0.0 baseline.
3. `cabal haddock hw-kafka-streamly` exits 0; only pre-existing upstream warnings remain.
4. `grep -rE "kafkaSource|kafkaSink|Kafka\.Streamly\.Source|Kafka\.Streamly\.Sink|kafkaBatchSink" hw-kafka-streamly hw-kafka-streamly-jitsurei mori.dhall README.md` returns nothing (after filtering out the historical plan files).
5. `cabal repl hw-kafka-streamly` followed by `:t Kafka.Streamly.Stream.kafkaStream` shows the expected `Stream m _` return type, and `:t Kafka.Streamly.Fold.kafkaFold` shows the expected `Fold m _ _` return type.
6. The cookbook help output (`cabal run hw-kafka-streamly-jitsurei`) lists `batch-fold` (not `batch-sink`) and references `kafkaStream` / `kafkaFold` in its summary text.
7. The CHANGELOG `0.2.0.0` section contains the exhaustive name-map table.
8. (Optional, when a broker is available) `cabal run streamly-producer` followed by `cabal run streamly-consumer` against a Redpanda broker produces five messages in and ten messages out (default cookbook config), demonstrating that the renamed library functions still work end-to-end.

A reviewer reading the resulting Hackage page (rendered locally as `cabal haddock`) sees `Kafka.Streamly.Stream` and `Kafka.Streamly.Fold` in the module list. Clicking through, they see only `kafkaStream*` and `kafkaFold*` exports; nothing references `Source` or `Sink` except the historical comparison sentence in `kafkaStream`'s docstring that names `hw-kafka-conduit`'s `kafkaSource` (a deliberate retention — the conduit name is a fact about a different package, not a stale reference to our own).


## Idempotence and Recovery

Every step is idempotent or trivially so. The file moves use `git mv`, which is reversible by `git mv` in the opposite direction; if a move is interrupted, run `git status` to see whether the source/destination exist and complete the move manually with another `git mv`. The textual Edit operations are file-local and produce a deterministic diff; if a `cabal build` failure shows a missed reference, search-and-replace the missing identifier and rebuild.

If the library milestone is committed but the test milestone fails partway, fix the failing reference in a follow-up commit on the same branch — `cabal test hw-kafka-streamly` is the canary. The library and test commits can in principle be combined; this plan keeps them separate only for clarity, and a reviewer-friendly single commit is also acceptable.

If the cookbook fails to build because a function reference was missed, re-run `cabal build all 2>&1 | grep -E "Variable not in scope|Module .* does not export"` to enumerate the missing references and patch them in one pass.

If, after Milestone 4, the Haddock build emits a warning that did not exist before, do not silence it — diagnose the cause. New warnings are most likely caused by a stale Haddock reference like `'Kafka.Streamly.Source'` left in a docstring; resolve by editing the docstring to point at `'Kafka.Streamly.Stream'`.

The `mori.dhall` description edit is purely textual; if `mori show --full` does not reflect the new description, run `dhall format --check mori.dhall` to ensure the file is well-formed and `mori show --full` again.

To roll back the entire plan: `git revert <hash>` of the single commit produced by milestone 6 restores the 0.1.0.0 names. The library is published as 0.1.0.0 on Hackage; reverting the rename in source does not affect what is on Hackage.


## Interfaces and Dependencies

The library and cookbook depend on `streamly-core` (>=0.3 && <0.5) and `hw-kafka-client` (>=5.3 && <6). No dependency change accompanies this plan. The streamly modules consumed are `Streamly.Data.Stream`, `Streamly.Data.Fold`, `Streamly.Data.Scanl`, all from `streamly-core`; the streamly types referenced in our type signatures (`Stream`, `Fold`) are those modules' exports. The hw-kafka-client modules consumed are `Kafka.Consumer` and `Kafka.Producer`; types referenced (`KafkaConsumer`, `KafkaProducer`, `KafkaError`, `ConsumerProperties`, `Subscription`, `Timeout`, `ProducerProperties`, `ProducerRecord`, `ConsumerRecord`) are unchanged.

End-state interface contracts (signatures) the milestones must produce.

After Milestone 1, `Kafka.Streamly.Stream` exports:

    kafkaStream
        :: (MonadIO m, MonadCatch m)
        => ConsumerProperties
        -> Subscription
        -> Timeout
        -> Stream m (Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))

    kafkaStreamAutoClose
        :: (MonadIO m, MonadCatch m)
        => KafkaConsumer
        -> Timeout
        -> Stream m (Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))

    kafkaStreamNoClose
        :: MonadIO m
        => KafkaConsumer
        -> Timeout
        -> Stream m (Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))

    isFatal, isPollTimeout, isPartitionEOF :: KafkaError -> Bool
    skipNonFatal      :: Monad m => Stream m (Either KafkaError b) -> Stream m (Either KafkaError b)
    skipNonFatalExcept :: Monad m => [KafkaError -> Bool]
                                  -> Stream m (Either KafkaError b)
                                  -> Stream m (Either KafkaError b)
    mapFirst   :: (Bifunctor t, Monad m) => (k -> k') -> Stream m (t k v) -> Stream m (t k' v)
    mapValue   :: (Functor t, Monad m)   => (v -> v') -> Stream m (t v)   -> Stream m (t v')
    bimapValue :: (Bifunctor t, Monad m) => (k -> k') -> (v -> v')
                                          -> Stream m (t k v) -> Stream m (t k' v')

`Kafka.Streamly.Fold` exports:

    kafkaFold       :: MonadIO m => KafkaProducer -> Fold m ProducerRecord (Maybe KafkaError)
    kafkaBatchFold  :: MonadIO m => KafkaProducer -> Fold m [ProducerRecord] (Maybe KafkaError)
    withKafkaProducer
        :: ProducerProperties
        -> (KafkaProducer -> IO a)
        -> IO (Either KafkaError a)

`Kafka.Streamly.Combinators` exports (unchanged):

    BatchSize(..)
    batchByOrFlush       :: Monad m => BatchSize -> Stream m (Maybe a) -> Stream m [a]
    batchByOrFlushEither :: Monad m => BatchSize -> Stream m (Either e a) -> Stream m [a]
    throwLeft         :: (MonadThrow m, Exception e) => Stream m (Either e a) -> Stream m a
    throwLeftSatisfy  :: (MonadThrow m, Exception e)
                      => (e -> Bool) -> Stream m (Either e a) -> Stream m (Either e a)

After Milestone 2, the test suite imports `Kafka.Streamly.Stream` (not `.Source`) and the test file is `hw-kafka-streamly/test/Kafka/Streamly/StreamTest.hs` exposing `tests :: TestTree` named `"Stream"`.

After Milestone 3, the executable list in `hw-kafka-streamly-jitsurei.cabal` is `hw-kafka-streamly-jitsurei`, `streamly-producer`, `streamly-consumer`, `error-handling`, `transform-pipeline`, `batch-fold`, `consume-produce`, `concurrent-consume`. The legacy `batch-sink` no longer exists.
