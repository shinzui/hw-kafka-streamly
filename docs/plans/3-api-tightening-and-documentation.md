# API tightening and documentation

MasterPlan: docs/masterplans/1-release-hw-kafka-streamly-0.1.0.0.md

Intention: intention_01knbcpkxqemdaawn3zzs822f8

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

After this plan is complete, the public API of `hw-kafka-streamly` is the one 0.1.0.0 will commit to: it has a bug fixed (double-flush in `withKafkaProducer`), it is input-validated where user input can trip it (`BatchSize` on `batchByOrFlush`), and every exported identifier carries Haddock that accurately describes what it does and, where relevant, the subtle semantics it inherits from `hw-kafka-client` or from the shape of a nested `Either KafkaError (ConsumerRecord k v)` stream. Users reading the Hackage page see a module-level overview on each of `Kafka.Streamly.Source`, `Kafka.Streamly.Sink`, and `Kafka.Streamly.Combinators`, with at least one worked example per module showing an end-to-end pipeline.

The misleading naming of the value-mapping helpers (`mapFirst`, `mapValue`, `bimapValue`, and nine sequence/traverse variants) is addressed: either the API is pruned to the subset that reliably matches user intent, or the docstrings are rewritten so the names cannot be misread. This decision is made in the Decision Log of this plan and applies to 0.1.0.0 going forward.

The plan delivers Haddock that renders correctly under `cabal haddock --haddock-for-hackage` with no warnings, and zero behavior change for any code written against the current API except for the `withKafkaProducer` double-flush fix (which is a pure optimization — no caller can observe a difference) and, potentially, the removal of some value-mapping helpers (a breaking API change deferred to this plan's Decision Log).


## Progress

- [x] Fix withKafkaProducer double-flush in hw-kafka-streamly/src/Kafka/Streamly/Sink.hs (2026-04-17)
- [x] Document flush semantics on kafkaSink and kafkaBatchSink (2026-04-17)
- [x] Document kafkaBatchSink sends per-record (not a broker-side batch) (2026-04-17)
- [x] Decide on the value-mapping helpers: prune, rename, or re-document (record in Decision Log) (2026-04-17 — option (b), prune)
- [x] Apply the value-mapping helpers decision in hw-kafka-streamly/src/Kafka/Streamly/Source.hs (2026-04-17)
- [x] Validate BatchSize input in Combinators.batchInternal (2026-04-17)
- [x] Expand skipNonFatal docstring to mention partition EOF (2026-04-17)
- [x] Document kafkaSource's throw-on-newConsumer-failure divergence from conduit (2026-04-17)
- [x] Add module-level Haddock to Kafka.Streamly.Source (2026-04-17)
- [x] Add module-level Haddock to Kafka.Streamly.Sink (2026-04-17)
- [x] Add module-level Haddock to Kafka.Streamly.Combinators (2026-04-17)
- [x] Add @since 0.1.0.0 to every exported identifier (2026-04-17 — 18 tags across three modules; BatchSize re-export covered by module header)
- [x] Verify cabal build all succeeds with no warnings (2026-04-17 — also fixed pre-existing unused-import warning in jitsurei/BatchSink.hs)
- [x] Verify cabal haddock hw-kafka-streamly succeeds (2026-04-17 — only pre-existing upstream warnings remain, see Surprises)
- [x] Append a brief summary of user-visible changes to hw-kafka-streamly/CHANGELOG.md (2026-04-17)
- [ ] Commit with ExecPlan + MasterPlan + Intention trailers


## Surprises & Discoveries

- Haddock emits `'KafkaError' is ambiguous` and `'BatchSize' is ambiguous`
  warnings when resolving identifiers in our type signatures. The ambiguity
  is inherited from upstream `hw-kafka-client`: each newtype/data type has
  both a type constructor and a data constructor with the same name exported
  from `Kafka.Types`, so Haddock cannot pick one when auto-linking. These
  warnings existed on `master` before this plan started (verified by
  `git stash && cabal haddock`). Fixable only at upstream or by rewriting
  every type signature to use alternative naming — both out of scope for
  0.1.0.0. My docstring references use the `t'…'` namespace prefix to avoid
  contributing any additional instances of the warning.
  Date: 2026-04-17

- Haddock reports "could not find link destinations for
  `Streamly.Internal.Data.Stream.Type.Stream`" etc. This is because the
  `streamly-core` package on this system was installed without Haddock
  documentation (Haddock also prints "The following packages have no
  Haddock documentation installed. No links will be generated to these
  packages: streamly-core-0.4.0"). Pre-existing environment condition, not
  a code issue. Users building docs locally can `cabal install --lib
  --haddock streamly-core` to resolve.
  Date: 2026-04-17

- `hw-kafka-streamly-jitsurei/app/BatchSink.hs` carried a pre-existing
  unused-import of `Streamly.Data.Fold`. Removed as part of this plan
  because the acceptance criterion demands a warning-free `cabal build all`.
  Date: 2026-04-17

- `cabal build all` prints one `-Wunused-packages` warning during a clean
  rebuild, naming `directory-1.3.9.0`. The warning is emitted by GHC while
  compiling `streamly-0.12.0` (observable from its position in the build
  output, immediately before `Streamly.Internal.Control.Concurrent`), not
  by any module in this repository. It reflects an upstream `build-depends`
  declaration in streamly that is no longer used by its compilation units
  on GHC 9.12.2. Not fixable from here; upstream issue.
  Date: 2026-04-17

- Inspection of `hw-kafka-streamly-jitsurei/app/TransformPipeline.hs`
  confirmed only `mapValue` is used from the 12 value-mapping helpers, and
  the other two demo patterns (prefix keys, bimap both) reach for inline
  `fmap (fmap (first …))` / `fmap (fmap (bimap …))` rather than any of the
  sequence/traverse variants. This is strong evidence for pruning the nine
  unused variants — recorded in the Decision Log.
  Date: 2026-04-17


## Decision Log

- Decision: The value-mapping helpers decision is deferred to when the work begins. The three options on the table, to be chosen between during implementation, are:

    (a) **Keep and re-document** — keep all 12 helpers (`mapFirst`, `mapValue`, `bimapValue`, `sequenceValueFirst`, `sequenceValue`, `bisequenceValue`, `traverseValueFirst`, `traverseValue`, `bitraverseValue`, `traverseValueFirstM`, `traverseValueM`, `bitraverseValueM`) but rewrite the Haddock to describe them as bifunctor/traversable lifts rather than "first element (key)" / "second element (value)" which is misleading in the nested `Either KafkaError (ConsumerRecord k v)` context.

    (b) **Prune aggressively** — keep only the two or three most commonly-useful helpers and delete the rest. In Streamly, users can write `fmap (fmap f)` inline, so most of these helpers are unused wallpaper. Candidates to keep: `mapValue`, `mapFirst`, `bimapValue`. Candidates to delete: all nine sequence/traverse variants.

    (c) **Add concrete `ConsumerRecord` helpers** — keep the generic 12 but add three concrete helpers (`mapRecordKey`, `mapRecordValue`, `bimapRecord`) that target the inner `ConsumerRecord` inside the outer `Either`, which is what users actually want.

  Rationale for deferral: the best option depends on how comfortable we are with a breaking change inside 0.1.0.0 before any public release. Since 0.1.0.0 is not yet on Hackage, breaking changes cost nothing. The leaning is (b) prune aggressively, but confirm during implementation by reviewing how the jitsurei examples actually use these helpers.
  Date: 2026-04-17

- Decision: Value-mapping helpers pruned per option (b). Keep `mapFirst`, `mapValue`, `bimapValue`; remove the nine sequence/traverse variants.
  Rationale: Inspection of `hw-kafka-streamly-jitsurei/app/TransformPipeline.hs` confirms only `mapValue` is used from the 12 helpers, and even that usage is awkward (`mapValue (fmap uppercaseBS)`). Patterns 2 and 3 of the same demo use inline `fmap (fmap (first …))` and `fmap (fmap (bimap …))` rather than the library's traverse/sequence variants, showing users reach for the obvious inline form first. Keeping nine unused functions in the public API of 0.1.0.0 commits us to maintaining them forever. Since 0.1.0.0 is unreleased, pruning now costs nothing. The three retained helpers cover Pattern 1 (map value), Pattern 2 (map key), Pattern 3 (bimap) with a clear vocabulary and can each be re-documented as bifunctor lifts over the outer stream element rather than "key"/"value" of anything.
  Date: 2026-04-17

- Decision: `withKafkaProducer` stays in `IO` for 0.1.0.0; `MonadUnliftIO`/`MonadMask` generalization is out of scope.
  Rationale: widening the monad constraint is a non-trivial type change and every caller today uses `IO`. Defer to 0.2.
  Date: 2026-04-17


## Outcomes & Retrospective

Implemented 2026-04-17 in a single change set spanning the three library
source files, one jitsurei example, and the CHANGELOG.

**Delivered:**

- `withKafkaProducer` cleanup simplified to `closeProducer` alone (flushes
  once instead of twice).
- `batchByOrFlush` / `batchByOrFlushEither` now reject non-positive
  `BatchSize` at the call site with an error naming the combinator.
- Value-mapping helper surface pruned from 12 to 3 (`mapFirst`, `mapValue`,
  `bimapValue`). Nine sequence/traverse variants removed. Jitsurei's
  `TransformPipeline.hs` continues to compile unchanged — it used only
  `mapValue`.
- Module-level Haddock added to all three modules with a worked example
  each, plus explicit notes on: flush semantics in the sinks,
  `kafkaBatchSink`'s per-record sends (with upstream rationale),
  `kafkaSource`'s throw-on-creation-failure divergence from
  `hw-kafka-conduit`, and `skipNonFatal`'s handling of partition EOF.
- `@since 0.1.0.0` on every exported identifier (18 tags across the three
  modules).
- `cabal build all` is clean (one pre-existing jitsurei warning also fixed
  in passing).
- `cabal check` on `hw-kafka-streamly` passes.
- `cabal haddock --haddock-for-hackage` emits only pre-existing warnings
  (documented in Surprises).

**Gaps against original purpose:**

- The acceptance criterion of a fully warning-free Haddock is not met due
  to upstream ambiguity (`KafkaError`, `BatchSize`) and the absence of
  installed `streamly-core` Haddocks on this workstation. Neither is
  addressable within this plan's scope.

**Lessons:**

- Starting from evidence in the jitsurei cookbook (which helpers are
  actually used) made the prune decision straightforward — the Decision
  Log was trivial once the grep results were in.
- Haddock's `t'Foo'` / `v'Foo'` namespace prefixes are the right tool for
  disambiguating when an upstream package re-exports a name in two
  namespaces. Single quotes alone (`'Foo'`) produce the ambiguity warning.


## Context and Orientation

The library lives at `/Users/shinzui/Keikaku/bokuno/hw-kafka-streamly`. Three source files constitute the entire library:

- `hw-kafka-streamly/src/Kafka/Streamly/Source.hs` — consumer stream sources and helpers
- `hw-kafka-streamly/src/Kafka/Streamly/Sink.hs` — producer folds and bracket helper
- `hw-kafka-streamly/src/Kafka/Streamly/Combinators.hs` — batching and error-throwing combinators

### Kafka.Streamly.Source — current state

Exports three stream sources plus error and value helpers:

    kafkaSource          :: (MonadIO m, MonadCatch m)
                         => ConsumerProperties -> Subscription -> Timeout
                         -> Stream m (Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))
    kafkaSourceAutoClose :: (MonadIO m, MonadCatch m)
                         => KafkaConsumer -> Timeout
                         -> Stream m (Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))
    kafkaSourceNoClose   :: MonadIO m
                         => KafkaConsumer -> Timeout
                         -> Stream m (Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))

Error predicates: `isFatal`, `isPollTimeout`, `isPartitionEOF`.

Error filters: `skipNonFatal`, `skipNonFatalExcept`.

Value helpers: `mapFirst`, `mapValue`, `bimapValue`, `sequenceValueFirst`, `sequenceValue`, `bisequenceValue`, `traverseValueFirst`, `traverseValue`, `bitraverseValue`, `traverseValueFirstM`, `traverseValueM`, `bitraverseValueM`. All are one-liners delegating to `fmap`, `first`, `bimap`, `sequenceA`, `bisequenceA`, `bitraverse`, or `traverse`.

`kafkaSource` behavior on consumer creation failure (Source.hs:96-104):

    kafkaSource props sub timeout =
        Stream.bracketIO
            ( newConsumer props sub >>= \case
                Left err -> throwIO err
                Right c -> pure c
            )
            (\c -> () <$ closeConsumer c)
            (\c -> kafkaSourceNoClose c timeout)

The throw-on-failure is a semantic divergence from `hw-kafka-conduit`'s `kafkaSource`, which yields the `Left err` as a stream value and then terminates. Users migrating from conduit should be told.

### Kafka.Streamly.Sink — current state

    kafkaSink          :: MonadIO m => KafkaProducer -> Fold m ProducerRecord (Maybe KafkaError)
    kafkaBatchSink     :: MonadIO m => KafkaProducer -> Fold m [ProducerRecord] (Maybe KafkaError)
    withKafkaProducer  :: ProducerProperties -> (KafkaProducer -> IO a) -> IO (Either KafkaError a)

The `withKafkaProducer` implementation (Sink.hs:65-76):

    withKafkaProducer props action =
        newProducer props >>= \case
            Left err -> pure (Left err)
            Right producer ->
                bracket
                    (pure producer)
                    (\p -> flushProducer p >> closeProducer p)
                    (\p -> Right <$> action p)

The bug: `hw-kafka-client`'s `Kafka.Producer.closeProducer` is literally defined as `flushProducer` (see `/Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project/hw-kafka-client/src/Kafka/Producer.hs:193-194`), so `flushProducer p >> closeProducer p` flushes twice. The fix is to drop the explicit `flushProducer p` — `closeProducer p` alone achieves the same effect.

The `kafkaBatchSink` implementation walks one record at a time (Sink.hs:47-57):

    kafkaBatchSink producer = Fold.foldlM' step (pure Nothing)
      where
        step Nothing batch = sendBatch batch
        step err@(Just _) _ = pure err
        sendBatch [] = pure Nothing
        sendBatch (r : rs) = do
            result <- liftIO $ produceMessage producer r
            case result of
                Nothing -> sendBatch rs
                Just err -> pure (Just err)

There is no broker-side batch RPC here — each record is a separate `produceMessage` call. The reason, per the original plan's Surprises section: `produceMessageBatch` is not exported from `hw-kafka-client` 5.3.0 on Hackage (verified by inspecting `/tmp/hw-kafka-client-5.3.0/src/Kafka/Producer.hs` after `cabal get`). This is documentation-worthy, not fixable in 0.1.0.0.

### Kafka.Streamly.Combinators — current state

    throwLeft         :: (MonadThrow m, Exception e) => Stream m (Either e a) -> Stream m a
    throwLeftSatisfy  :: (MonadThrow m, Exception e) => (e -> Bool) -> Stream m (Either e a) -> Stream m (Either e a)
    batchByOrFlush       :: Monad m => BatchSize -> Stream m (Maybe a) -> Stream m [a]
    batchByOrFlushEither :: Monad m => BatchSize -> Stream m (Either e a) -> Stream m [a]

`BatchSize` is a newtype from `Kafka.Types` re-exported by `Combinators`.

The batching internal (Combinators.hs:82-102) drives a state machine `(Int, [a], Maybe [a])`. Key line:

    step (i, acc, _) (Just a) =
        let acc' = a : acc
         in if i + 1 >= n
                then (0, [], Just (reverse acc'))
                else (i + 1, acc', Nothing)

With `BatchSize 0`, every `Just` element triggers `i + 1 >= 0` (true) and emits a singleton batch. With negative values the same thing happens. Both are nonsensical inputs that the function nevertheless produces output for. EP-3 fixes this with a guard at the call site.

### Context from the jitsurei cookbook on value-helper usage

Two call sites use the value-mapping helpers in `hw-kafka-streamly-jitsurei/app/`:

- `TransformPipeline.hs` line 71: `mapValue (fmap uppercaseBS)` — here the user passes `fmap uppercaseBS` so the outer Either's Right (a ConsumerRecord) has its value transformed. The double-`fmap` is what the user actually wrote inline.
- `TransformPipeline.hs` line 79 and line 88: uses `fmap (fmap (first prefixKey))` and `fmap (fmap (bimap prefixKey uppercaseBS))` inline — does not use `mapFirst`/`bimapValue` at all.

So in practice, of the 12 helpers, only `mapValue` is used, and its usage is awkward. This is evidence for option (b) — prune aggressively — in the Decision Log.


## Plan of Work

Six edit clusters. Each can be its own commit if desired, but a single commit for the whole plan is also acceptable since everything must land together for the API to be coherent.


### 1. Fix `withKafkaProducer` double-flush

File: `hw-kafka-streamly/src/Kafka/Streamly/Sink.hs`.

Replace the cleanup action `(\p -> flushProducer p >> closeProducer p)` with `(\p -> closeProducer p)` (or equivalently, just `closeProducer`).

The `flushProducer` import can be removed if no other use remains. Verify by grepping the file.


### 2. Document flush semantics on `kafkaSink` and `kafkaBatchSink`

File: `hw-kafka-streamly/src/Kafka/Streamly/Sink.hs`.

Expand the Haddock on `kafkaSink` and `kafkaBatchSink` to state explicitly that a successful fold (`Nothing` result) means all `produceMessage` calls succeeded — which only means librdkafka has *queued* the messages, not that they have reached the broker. Direct the user at `withKafkaProducer` (or `Kafka.Producer.flushProducer` + `closeProducer`) for end-to-end delivery guarantees.

Also note that after an error, the fold still consumes remaining input but sends nothing (current behavior — is already documented, keep).


### 3. Document `kafkaBatchSink` is not a broker-side batch

File: `hw-kafka-streamly/src/Kafka/Streamly/Sink.hs`.

Expand the `kafkaBatchSink` Haddock:

> Note: as of `hw-kafka-client-5.3.0`, the batch is sent as individual `produceMessage` calls because `produceMessageBatch` is not exported from that version. A future release may switch to a true broker-side batch send once the upstream dependency supports it. Today this fold is a convenience for accepting `[ProducerRecord]` input (e.g., the output of `Kafka.Streamly.Combinators.batchByOrFlush`) — it does not reduce network round-trips.


### 4. Value-mapping helpers: apply the chosen decision

First, record the chosen option in the Decision Log of this plan (above) with rationale from inspecting the jitsurei usage.

File: `hw-kafka-streamly/src/Kafka/Streamly/Source.hs`.

If option (a) — keep and re-document — rewrite each helper's Haddock. Replace phrases like "Map over the first element (key)" with language like "Lift a function into the left position of a `Bifunctor`." Do not mention "key" or "value" unless the helper actually targets a `ConsumerRecord` field. Add a module-level note showing a typical usage against the nested `Either KafkaError (ConsumerRecord k v)` shape with `fmap (fmap f)` rather than these helpers.

If option (b) — prune aggressively — remove all nine sequence/traverse variants from both the source file and the export list. Keep `mapFirst`, `mapValue`, `bimapValue`. Verify `cabal build all` still succeeds. If `TransformPipeline.hs` was using any removed helper, update it to inline `fmap`/`bimap` equivalents.

If option (c) — add concrete helpers — keep all 12 and add three new ones:

    -- | Map over the key of a 'ConsumerRecord' inside an 'Either' layer.
    mapRecordKey   :: Monad m => (k -> k') -> Stream m (Either e (ConsumerRecord k v)) -> Stream m (Either e (ConsumerRecord k' v))
    mapRecordValue :: Monad m => (v -> v') -> Stream m (Either e (ConsumerRecord k v)) -> Stream m (Either e (ConsumerRecord k v'))
    bimapRecord    :: Monad m => (k -> k') -> (v -> v') -> Stream m (Either e (ConsumerRecord k v)) -> Stream m (Either e (ConsumerRecord k' v'))

Update exports accordingly.

Record the choice in the Decision Log with rationale before editing.


### 5. Validate `BatchSize` input in `batchInternal`

File: `hw-kafka-streamly/src/Kafka/Streamly/Combinators.hs`.

Add a guard at the top of `batchInternal`: if `n <= 0`, either throw a runtime error at the call site, or treat as `n = 1` (one-element batches), or simply document that behavior is undefined for `n <= 0`. The recommendation is to `error "batchByOrFlush: BatchSize must be positive"` — immediate and loud failure is better than silent wrong-sized batches. The `error` message must name which combinator surfaced the failure.

Apply in both `batchByOrFlush` and `batchByOrFlushEither` (which share `batchInternal`).


### 6. Module-level Haddock, usage examples, `@since` tags

Files: all three source files.

Add a module-level Haddock block at the top of each file (above the module declaration). Each block should include:

- A short synopsis.
- For `Source`: a brief explanation of the three-tier resource management (fully-managed `kafkaSource`, auto-close `kafkaSourceAutoClose`, unmanaged `kafkaSourceNoClose`), and a worked example: six lines that create a consumer and fold the stream.
- For `Sink`: an explanation of why sinks are `Fold` values (dual of Stream in Streamly) and why `withKafkaProducer` is provided as a separate bracket. A worked example: produce five records, folded over the sink.
- For `Combinators`: a one-paragraph statement that this module is auxiliary — batching when consumer output needs to be sent as `[ProducerRecord]`, and exception combinators for callers who prefer exceptions over `Either`.

Append `@since 0.1.0.0` to every exported identifier's Haddock. This is boilerplate but cheap and useful for future changelogs.

Also expand `skipNonFatal`'s docstring to mention that it drops `RdKafkaRespErrPartitionEof`, since users may be surprised — EOF is technically non-fatal, but some consumers rely on it.

Document `kafkaSource`'s throw-on-newConsumer-failure behavior explicitly in its Haddock, including a comparison bullet with `hw-kafka-conduit`'s behavior (yields `Left err` then terminates).


## Concrete Steps

All commands run from the repository root `/Users/shinzui/Keikaku/bokuno/hw-kafka-streamly` unless noted.

Step 1. Inspect the current state of the three source files:

    wc -l hw-kafka-streamly/src/Kafka/Streamly/*.hs

Step 2. Read the jitsurei usage to ground the Decision Log for the value-mapping helpers:

    grep -n "mapValue\|mapFirst\|bimapValue\|sequenceValue\|traverseValue\|bitraverseValue" hw-kafka-streamly-jitsurei/app/*.hs

Record findings in the Decision Log. Choose option (a), (b), or (c). Proceed with the chosen edits.

Step 3. Apply edits 1 through 6 per the Plan of Work.

Step 4. Build and verify:

    cabal build all

Expected: clean build with no warnings, no errors.

Step 5. Generate Haddock and check for warnings:

    cabal haddock hw-kafka-streamly --haddock-for-hackage

Expected: Haddock completes with no warnings about missing docs, malformed markup, or broken links.

Step 6. Spot-check the rendered Haddock by opening the produced HTML (the command prints the path, typically under `dist-newstyle/.../doc/html/hw-kafka-streamly/`). Confirm the module-level docs render and at least one exported function shows `@since 0.1.0.0`.

Step 7. Append a brief bullet list to `hw-kafka-streamly/CHANGELOG.md` under the 0.1.0.0 (unreleased) section's "### Fixed" and "### Changed" subsections as appropriate:

    ### Fixed
    - `withKafkaProducer` no longer flushes twice on producer teardown.

    ### Changed
    - Value-mapping helpers documentation tightened (see module haddock).
    - `batchByOrFlush`/`batchByOrFlushEither` now reject non-positive `BatchSize`.

(Adjust to match actual edits chosen.)

Step 8. Commit:

    git add hw-kafka-streamly/src/Kafka/Streamly/Source.hs \
            hw-kafka-streamly/src/Kafka/Streamly/Sink.hs \
            hw-kafka-streamly/src/Kafka/Streamly/Combinators.hs \
            hw-kafka-streamly/CHANGELOG.md
    git commit -m "$(cat <<'EOF'
    Tighten hw-kafka-streamly public API and Haddock for 0.1.0.0

    - Fix withKafkaProducer double-flush (closeProducer already flushes).
    - Document flush semantics on kafkaSink/kafkaBatchSink.
    - Document kafkaBatchSink sends per-record (produceMessageBatch is
      not exported from hw-kafka-client 5.3.0).
    - <value-helpers decision summary>.
    - Reject non-positive BatchSize in batchByOrFlush/batchByOrFlushEither.
    - Add module-level Haddock and @since 0.1.0.0 tags.

    MasterPlan: docs/masterplans/1-release-hw-kafka-streamly-0.1.0.0.md
    ExecPlan: docs/plans/3-api-tightening-and-documentation.md
    Intention: intention_01knbcpkxqemdaawn3zzs822f8
    EOF
    )"


## Validation and Acceptance

Acceptance is:

1. `cabal build all` produces zero warnings and zero errors.

2. `cabal haddock hw-kafka-streamly --haddock-for-hackage` completes with zero warnings.

3. `grep -c "@since" hw-kafka-streamly/src/Kafka/Streamly/*.hs` is at least 20 (one per exported identifier after the decision is applied; exact count depends on whether helpers were pruned).

4. The fix for `withKafkaProducer` is verifiable by inspection: `grep -A3 "withKafkaProducer" hw-kafka-streamly/src/Kafka/Streamly/Sink.hs` shows `closeProducer` as the sole cleanup action.

5. `cd hw-kafka-streamly && cabal check` continues to pass.

6. If `TransformPipeline.hs` or any other jitsurei example relied on a removed helper, it still compiles (rewritten to use `fmap`/`bimap` inline).

7. The Decision Log of this plan records which value-mapping-helpers option was chosen and why.


## Idempotence and Recovery

Each edit cluster is self-contained and idempotent. If the build breaks mid-plan, revert the individual file and retry. The Haddock generation is lossless and can be re-run freely.

If the value-mapping decision needs revisiting after implementation begins (because inspecting the code reveals new evidence), record the revised decision in the Decision Log with date and rationale, then apply the new choice.


## Interfaces and Dependencies

No new external dependencies.

Signatures that must exist at the end of this plan, in `hw-kafka-streamly/src/Kafka/Streamly/Sink.hs`:

    withKafkaProducer :: ProducerProperties -> (KafkaProducer -> IO a) -> IO (Either KafkaError a)

Unchanged in type; behavior corrected to flush only once.

Signatures that must exist in `hw-kafka-streamly/src/Kafka/Streamly/Combinators.hs`:

    batchByOrFlush       :: Monad m => BatchSize -> Stream m (Maybe a)   -> Stream m [a]
    batchByOrFlushEither :: Monad m => BatchSize -> Stream m (Either e a) -> Stream m [a]

Unchanged in type; both now reject non-positive `BatchSize` via `error`.

Signatures in `hw-kafka-streamly/src/Kafka/Streamly/Source.hs` depend on the value-helpers decision — document the final surface in the Decision Log and match it here before closing out the plan.
