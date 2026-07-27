---
id: 1
slug: release-hw-kafka-streamly-0.1.0.0
title: "Release hw-kafka-streamly 0.1.0.0"
kind: master-plan
created_at: 2026-04-17T22:28:34Z
intention: "intention_01knbcpkxqemdaawn3zzs822f8"
---


# Release hw-kafka-streamly 0.1.0.0

This MasterPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `claude/skills/master-plan/MASTERPLAN.md`.


## Vision & Scope

After this initiative is complete, `hw-kafka-streamly` version 0.1.0.0 is published as a Hackage candidate with a stable, documented public API, a green test suite, polished cookbook examples that have been run end-to-end against a local Redpanda cluster, and cabal metadata that lets Hackage render a useful package page. A Haskell developer who does not know this repository can visit the Hackage page, read the README, scan the module-level Haddock for `Kafka.Streamly.Source`, `Kafka.Streamly.Sink`, and `Kafka.Streamly.Combinators`, and write their own consume/produce pipeline using composable Streamly streams and folds.

The scope covers everything the prior review identified as blocking or soft-blocking an initial release: (1) missing packaging artifacts and cabal metadata, (2) API rough edges and documentation gaps, (3) absence of any automated tests, (4) unvalidated cookbook examples plus one example that silently drops fatal errors, and (5) the fact that the library currently pins to unpublished versions of `streamly-core` and `streamly`, which makes it impossible to solve as-is from Hackage.

The scope explicitly excludes: adding new consumer or producer features beyond what 0.1.0.0 already ships (no transactions, no schema-registry integration, no OpenTelemetry), changing the three-tier resource model, switching streaming libraries, and adding Windows/ARM support. Those belong to later minor releases.


## Decomposition Strategy

The review produced five natural work streams that map cleanly onto functional concerns and can be verified independently. The decomposition was driven by three principles: (a) keep packaging and metadata edits separate from API edits so the former can ship without blocking on API decisions, (b) isolate automated tests into their own plan so the test scaffolding is treated as a first-class deliverable rather than a side effect, and (c) treat the upstream-dependency question and the Hackage upload as one coordinated release gate because they interlock (you cannot upload candidate sdists while `build-depends` references unpublished packages).

The five work streams are:

1. **EP-1 Release packaging and metadata** — safe, mechanical edits: README, CHANGELOG, cabal metadata fields, dropping the unused `bifunctors` dependency, a cosmetic cabal synopsis fix on the jitsurei package. No behavior change; no API change. Can begin immediately.

2. **EP-2 API tightening and documentation** — the one plan that makes user-visible changes to the public API: fixing the `withKafkaProducer` double-flush bug, tightening Haddock on every exported function, resolving the misleading `mapFirst`/`mapValue` naming, pruning the 12-function value-mapping bundle down to something defensible, validating `BatchSize` input, and adding module-level Haddock with worked examples. Produces the stable API contract that 0.1.0.0 commits to.

3. **EP-3 Pure test suite** — the first automated tests in the project. A `tasty` + `tasty-hunit` + `tasty-quickcheck` suite exercising pure logic only: error predicates (`isFatal`, `isPollTimeout`, `isPartitionEOF`), error filters (`skipNonFatal`, `skipNonFatalExcept`), batching combinators (`batchByOrFlush`, `batchByOrFlushEither`), and exception combinators (`throwLeft`, `throwLeftSatisfy`). No Kafka broker involved; runs under `cabal test` on every build.

4. **EP-4 Jitsurei hardening and live validation** — fix `ConsumeProduce.hs` so it no longer silently drops fatal errors, add explanatory comments where example behavior diverges from naive expectation (concurrent ordering), then run all seven executables against a Redpanda cluster started via the checked-in `process-compose.yaml` and record observed transcripts. This is the last unchecked item in the original streamly-bindings ExecPlan at `docs/plans/1-streamly-bindings-for-hw-kafka-client.md`.

5. **EP-5 Upstream dependencies and Hackage release** — resolve the fact that `streamly-core >= 0.4 && < 0.5` and `streamly >= 0.12` are not on Hackage (only `streamly-core-0.3.0` and `streamly-0.11.x` are published as of 2026-04-17). Either wait for the upstream packages to publish, widen bounds if compatible, or vendor temporarily. Once Hackage-solvable, produce a candidate sdist with `cabal sdist`, Haddock with `cabal haddock --haddock-for-hackage`, and upload as a Hackage candidate tagged 0.1.0.0.

Alternatives considered and rejected: (a) merging EP-1 and EP-2 into a single "release prep" plan was rejected because it would conflate mechanical edits with API decisions and slow down the fast, safe ones; (b) merging EP-3 into EP-2 was rejected because writing the first test suite in a project is itself a meaningful scaffolding task that deserves its own scope; (c) splitting EP-5 into "bound resolution" and "upload" was rejected because the sdist produced must reflect whichever bound decision was made — the two are one decision; (d) combining EP-4 into EP-5 as "pre-release validation" was rejected because live validation can and should happen before the upstream-blockers are resolved, and finding bugs during EP-4 may feed back into EP-2.

The decomposition lands at five child plans, within the recommended two-to-seven range. No phasing is needed.


## Exec-Plan Registry

| # | Title | Path | Hard Deps | Soft Deps | Status |
|---|-------|------|-----------|-----------|--------|
| 2 | Release packaging and metadata | docs/plans/2-release-packaging-and-metadata.md | None | None | Complete |
| 3 | API tightening and documentation | docs/plans/3-api-tightening-and-documentation.md | None | None | Complete |
| 4 | Pure test suite | docs/plans/4-pure-test-suite.md | None | EP-3 | Complete |
| 5 | Jitsurei hardening and live validation | docs/plans/5-jitsurei-hardening-and-validation.md | None | EP-3 | Complete |
| 6 | Upstream dependencies and Hackage release | docs/plans/6-upstream-dependencies-and-hackage-release.md | EP-2, EP-3, EP-4, EP-5 | None | Complete |

Status values: Not Started, In Progress, Complete, Cancelled. Hard Deps and Soft Deps reference other rows by their # prefix.

Note on numbering: the existing ExecPlan `docs/plans/1-streamly-bindings-for-hw-kafka-client.md` occupies number 1. New child plans for this MasterPlan start at 2 and continue sequentially, per the ExecPlan naming convention.


## Dependency Graph

EP-2 (packaging), EP-3 (API), EP-4 (tests), and EP-5 (jitsurei) have no hard dependencies on each other and can all begin in parallel. EP-4 (tests) has a soft dependency on EP-3 (API): if EP-3 prunes the value-mapping helper bundle, tests must not exercise the removed functions. In practice, tests should be written against the surface agreed in EP-3; if EP-4 is started before EP-3 lands, its tests may need a small adjustment pass later. EP-5 (jitsurei) has a soft dependency on EP-3 for the same reason: cookbook examples that use pruned helpers would fail to compile. EP-5 should be started after EP-3's Decision Log records which helpers will be kept.

EP-6 (Hackage release) is the gate and has hard dependencies on EP-3 (so the API it ships is the stable 0.1.0.0 API), EP-4 (so the tests cabal-test runs are green), EP-5 (so the cookbook examples shipped in the sdist work against a real broker), and EP-2 (so cabal metadata is complete). EP-6 cannot begin until all four upstream plans are Complete.

Parallelism opportunity: EP-2 and EP-3 can run on two separate branches or sessions with near-zero conflict — they touch disjoint files (EP-2 edits `hw-kafka-streamly.cabal` metadata, creates root-level `README.md` / `CHANGELOG.md`; EP-3 edits `hw-kafka-streamly/src/Kafka/Streamly/*.hs` and Haddock). The one file both touch is `hw-kafka-streamly.cabal` — EP-2 edits metadata fields and the `build-depends` stanza, EP-3 may add Haddock-related flags. Conflicts are mechanical and easy to resolve.


## Integration Points

**hw-kafka-streamly.cabal** — touched by EP-2 (metadata, remove `bifunctors`, add `extra-doc-files`), EP-3 (possibly add a Haddock flags line), EP-4 (add `test-suite` stanza with `tasty` dependencies). EP-2 defines the baseline metadata; EP-3 and EP-4 add their stanzas without disturbing EP-2's fields. When resolving merge conflicts, prefer the union of all three plans' edits.

**README.md** (repository root) — created by EP-2. EP-5 may reference it in release notes. EP-2 owns its content; EP-5 consumes it read-only.

**CHANGELOG.md** (repository root) — created by EP-2 with an initial `## 0.1.0.0 (unreleased)` section. EP-3 and EP-4 should append to this section as they land user-visible changes. EP-6 replaces `(unreleased)` with the release date when tagging.

**hw-kafka-streamly/src/Kafka/Streamly/Source.hs** and **Sink.hs** and **Combinators.hs** — edited primarily by EP-3. EP-4 reads these as the test subject but does not modify them. If a test in EP-4 reveals a bug, the fix goes through EP-3.

**docs/plans/1-streamly-bindings-for-hw-kafka-client.md** — the original ExecPlan still has an unchecked "Validate examples against running Redpanda (manual test)" item. EP-5 completes this item and updates the original plan's Progress section accordingly.

**process-compose.yaml** (repository root) and **Justfile** (repository root) — consumed read-only by EP-5. Provide the Redpanda cluster used for live validation.


## Progress

- [x] EP-2: Add README.md with overview, install snippet, and minimal usage example
- [x] EP-2: Add CHANGELOG.md with initial 0.1.0.0 (unreleased) entry
- [x] EP-2: Add cabal metadata (homepage, bug-reports, source-repository, copyright, tested-with, extra-doc-files)
- [x] EP-2: Remove unused bifunctors build-depends
- [x] EP-2: Fix jitsurei cabal synopsis to clarify it is the cookbook executables package
- [x] EP-2: Verify cabal check passes cleanly
- [x] EP-3: Fix withKafkaProducer double-flush
- [x] EP-3: Document flush semantics on kafkaSink and kafkaBatchSink
- [x] EP-3: Document kafkaBatchSink sends per-record (not via produceMessageBatch)
- [x] EP-3: Resolve mapFirst/mapValue naming confusion
- [x] EP-3: Prune or defend the 12 value-mapping helpers
- [x] EP-3: Validate BatchSize input (guard on zero/negative)
- [x] EP-3: Expand skipNonFatal docstring to mention partition EOF
- [x] EP-3: Document kafkaSource's throw-on-newConsumer-failure divergence from conduit
- [x] EP-3: Add module-level Haddock to Source, Sink, Combinators
- [x] EP-3: Add @since 0.1.0.0 annotations to every exported identifier
- [x] EP-4: Add test-suite stanza and tasty framework
- [x] EP-4: Tests for isFatal, isPollTimeout, isPartitionEOF
- [x] EP-4: Tests for skipNonFatal, skipNonFatalExcept
- [x] EP-4: Tests for batchByOrFlush, batchByOrFlushEither
- [x] EP-4: Tests for throwLeft, throwLeftSatisfy
- [x] EP-4: cabal test passes with zero warnings
- [x] EP-5: Fix ConsumeProduce.hs to not drop fatal errors silently
- [x] EP-5: Annotate ConcurrentConsume.hs re: ordering
- [x] EP-5: Start Redpanda via process-compose and run all 7 executables
- [x] EP-5: Record observed transcripts in a validation log
- [x] EP-5: Mark the "Validate examples against running Redpanda" item complete in docs/plans/1-streamly-bindings-for-hw-kafka-client.md
- [x] EP-6: Resolve streamly-core and streamly version availability
- [x] EP-6: cabal build all against Hackage-only bounds
- [x] EP-6: cabal sdist and cabal haddock --haddock-for-hackage succeed
- [x] EP-6: Upload 0.1.0.0 candidate to Hackage
- [x] EP-6: Tag v0.1.0.0 in git


## Surprises & Discoveries

Recorded during the pre-initiative review (2026-04-17):

- `hw-kafka-client` version 5.3.0 as published on Hackage does not export `produceMessageBatch`, even though the local source tree at `/Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project/hw-kafka-client/src/Kafka/Producer.hs` does. The `kafkaBatchSink` implementation in this repo therefore sends records one-by-one via `produceMessage`; the function name promises batching semantics the library cannot deliver against Hackage 5.3.0. EP-3 must document this in the Haddock.

  **Correction (2026-07-27).** The framing above reads this as Hackage lagging a source tree that has the function. It is the reverse, and the mistake has since propagated. Upstream *deleted* `produceMessageBatch` in `72e6f6d` (October 2021), before the `v5.3.0` tag; it is absent from Hackage 5.3.0 *and* from upstream `main`. The corpus tree has it only because it was re-added there locally in `3599eb1` (2026-03-27) so the `hw-kafka-conduit` cookbook examples would compile. So there is no upstream release to wait for. Nor would the deleted function have helped: it was a `mapM` over `produceMessage`. Real batching needs a binding for librdkafka's `rd_kafka_produce_batch`, which the package has never had — now tracked as upstream issue `hw-kafka-client-no-produce-batch-binding` in this project's `mori/upstream-issues.dhall`.

- `Kafka.Producer.closeProducer = flushProducer` (verbatim, `/Users/shinzui/Keikaku/hub/haskell/hw-kafka-client-project/hw-kafka-client/src/Kafka/Producer.hs:193-194`). The current `withKafkaProducer` helper calls `flushProducer p >> closeProducer p`, i.e. it flushes twice. EP-3 fixes this.

- `-Wunused-packages` confirms `bifunctors` is declared but unused in `hw-kafka-streamly/hw-kafka-streamly.cabal` — `Data.Bifunctor` and `Data.Bitraversable` come from `base`. EP-2 removes it.

- `streamly-core` on Hackage is at 0.3.0; the cabal.project pins to a local checkout of 0.4.0. The library therefore cannot be solved from Hackage alone today. EP-6 addresses this.


## Decision Log

- Decision: Use five child plans (EP-2 through EP-6), not fewer.
  Rationale: Each maps onto a single functional concern (packaging, API, tests, examples, release mechanics). Merging any two would conflate work with different risk profiles — EP-2 is mechanical, EP-3 involves API decisions, EP-4 is framework scaffolding, EP-5 needs a live broker, EP-6 is upload mechanics. Keeping them separate means each can proceed as soon as its dependencies are satisfied.
  Date: 2026-04-17

- Decision: EP-6 is the only plan with hard dependencies. All others have at most soft dependencies.
  Rationale: Hackage upload is the only step that requires every other deliverable to be in place simultaneously. EP-2, EP-3, EP-4, EP-5 all produce value independently and can be shipped in any order.
  Date: 2026-04-17

- Decision: Number child plans starting at 2, continuing the existing ExecPlan sequence.
  Rationale: `docs/plans/1-streamly-bindings-for-hw-kafka-client.md` already exists. The ExecPlan naming convention (sequential numbering, slug from title) is per-directory, not per-MasterPlan, so this initiative's child plans become 2 through 6.
  Date: 2026-04-17

- Decision: Associate all plans with intention `intention_01knbcpkxqemdaawn3zzs822f8`.
  Rationale: User confirmed the same intention as the original streamly-bindings plan. This initiative is the release-readiness continuation of that work.
  Date: 2026-04-17

- Decision: `kafkaBatchSink`'s non-batching implementation stays for 0.1.0.0; EP-3 documents the limitation rather than fixing it.
  Rationale: The fix requires either waiting for a hw-kafka-client release that exposes `produceMessageBatch` (out of scope), or working around it in ways that would change the type signature (also out of scope for a bugfix). Documentation is the right response for 0.1.0.0.
  Date: 2026-04-17

- Decision: The 12-function value-mapping helper bundle in `Kafka.Streamly.Source` is a candidate for pruning in EP-3 but the decision is deferred to the EP-3 Decision Log.
  Rationale: The review flagged them as largely redundant (Streamly's `Stream` is a `Functor`, so users can write `fmap (fmap f)` inline), but pruning them breaks API parity with `hw-kafka-conduit` which exports the same names. EP-3 owns the call, to be made with full context.
  Date: 2026-04-17


## Outcomes & Retrospective

`hw-kafka-streamly-0.1.0.0` shipped to Hackage on 2026-04-17 with Haddock,
git tag `v0.1.0.0` on origin, and CHANGELOG marking the release date.
All five child plans (EP-2 through EP-6) are Complete.

### What was delivered

- **Package on Hackage**:
  <https://hackage.haskell.org/package/hw-kafka-streamly-0.1.0.0> renders a
  README, module list (three public modules), Haddock with 100% coverage,
  bug-tracker link, and source-repository link.
- **Stable 0.1.0.0 API** (EP-3): three consumer stream variants
  (`kafkaSource` / `kafkaSourceAutoClose` / `kafkaSourceNoClose`), two
  producer folds (`kafkaSink`, `kafkaBatchSink`), a `withKafkaProducer`
  bracket helper, error predicates and filters, value-mapping helpers
  pruned to three defensible ones (`mapFirst`, `mapValue`, `bimapValue`),
  and `BatchSize`-validated batching combinators. `withKafkaProducer` no
  longer flushes twice on teardown, every exported identifier carries
  `@since 0.1.0.0`, and every module has worked-example Haddock.
- **Automated tests** (EP-4): 58 tasty tests covering all pure logic;
  `cabal test` runs them in <1 s under CI-shaped conditions.
- **Validated cookbook** (EP-5): seven jitsurei executables run end-to-end
  against a local Redpanda cluster; transcripts recorded in
  `docs/validation/0.1.0.0-jitsurei.md`. The silent-error bug in
  `ConsumeProduce.hs` is fixed.
- **Hackage-solvable metadata** (EP-2 + EP-6): README, CHANGELOG, cabal
  metadata (homepage, bug-reports, source-repository, copyright,
  tested-with, extra-doc-files), unused `bifunctors` dep dropped,
  `streamly-core >= 0.3 && < 0.5` widened to match published versions,
  `optional-packages:` local override removed so `cabal install
  hw-kafka-streamly` resolves from Hackage alone.

### Against the original vision

Every blocker the pre-initiative review identified is resolved. A Haskell
developer who does not know this repo can now visit the Hackage page, read
the README, scan the three modules' Haddock, and write their own consume
or produce pipeline. `cabal install hw-kafka-streamly` works against
Hackage (given `librdkafka` installed for `hw-kafka-client`). Scope
exclusions held: no new transactions / schema-registry / OpenTelemetry
features, no Windows/ARM support, the three-tier resource model is
unchanged, and the streaming library remains Streamly.

### What remains

- Future polish (not shipped for 0.1.0.0):
    - Fix Haddock's cosmetic `KafkaError` ambiguity and missing
      `Rep_BatchSize` link warnings.
    - Switch `kafkaBatchSink` to `produceMessageBatch` once a
      `hw-kafka-client` release exposes it (today it sends per-record
      under a batching type signature).
    - Re-tighten the streamly bound once `streamly-core-0.4` /
      `streamly-0.12` land on Hackage.
- No follow-up MasterPlan is planned for those; they are tracked as
  opportunities rather than commitments.

### Lessons learned

- **Split packaging from API**: EP-2 shipped mechanical edits fast and
  unblocked reviewers while EP-3 negotiated the API surface. If we had
  merged them we would have waited on the slowest of the two.
- **First tests as their own plan**: EP-4 treated the test-suite
  scaffolding as a deliverable rather than a side effect of EP-3. This
  made it obvious when the suite was "done" instead of drifting as API
  changes landed.
- **Dependency widening is often free**: the streamly API churn from
  0.3 → 0.4 did not touch anything this library uses. Checking the
  published-version exports before assuming a vendor/wait path saved a
  significant chunk of work in EP-6.
- **Gate user-visible actions explicitly**: both Hackage uploads and the
  git tag were held behind explicit user approval, per the ExecPlan
  convention. Once the candidate rendered correctly and the user
  confirmed, promotion to a full release was a single command.
