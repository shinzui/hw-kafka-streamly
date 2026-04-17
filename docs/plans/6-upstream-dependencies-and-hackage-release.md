# Upstream dependencies and Hackage release

MasterPlan: docs/masterplans/1-release-hw-kafka-streamly-0.1.0.0.md

Intention: intention_01knbcpkxqemdaawn3zzs822f8

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

After this plan is complete, `hw-kafka-streamly` version 0.1.0.0 is uploaded to Hackage as a candidate (or full release), tagged `v0.1.0.0` in git, and solvable from `cabal install hw-kafka-streamly` on a fresh machine. The upstream-dependency problem is resolved: either the local-checkout references in `cabal.project` are removed because `streamly-core-0.4` and `streamly-0.12` have been published by Composewell, or the bounds in `hw-kafka-streamly.cabal` are widened to accept the highest currently-published versions (`streamly-core-0.3.x`), or — if neither is viable — a narrower alternative path is documented and executed (e.g., vendoring). The sdist produced by `cabal sdist` builds cleanly from Hackage in an empty cabal store.

This is the release gate. It runs last because its success depends on the API being stable (EP-3), tests being green (EP-4 ≈ EP-3 review's test plan), and cookbook examples being validated (EP-5). It inherits whatever bound and behavior decisions those plans landed.

The novice reader reaching this plan has: a polished library, a README, a CHANGELOG, a test suite, and observed evidence that the cookbook works against a real broker. What remains is the Hackage mechanics — which is not a single command but a sequence of verifications.


## Progress

- [x] Check current Hackage-available versions of streamly-core and streamly (2026-04-17)
- [x] Decide on the bound strategy (record in Decision Log): wait, widen, or vendor (2026-04-17 — widen)
- [x] Apply bound strategy to hw-kafka-streamly/hw-kafka-streamly.cabal and cabal.project (2026-04-17)
- [x] Verify cabal build all succeeds with the new bounds (2026-04-17; all 58 tests pass)
- [x] Remove or comment the optional-packages block in cabal.project if no longer needed (2026-04-17 — removed, comment notes how to re-add for local-dev)
- [x] Produce sdist: cabal sdist hw-kafka-streamly (2026-04-17; dist-newstyle/sdist/hw-kafka-streamly-0.1.0.0.tar.gz)
- [x] Produce Haddock: cabal haddock hw-kafka-streamly --haddock-for-hackage (2026-04-17; 100% coverage, dist-newstyle/hw-kafka-streamly-0.1.0.0-docs.tar.gz)
- [x] Inspect the sdist tarball contents (README.md, CHANGELOG.md, cabal file, sources) (2026-04-17; README+CHANGELOG+LICENSE+3 src modules+test tree present, jitsurei/docs/flake/Justfile absent)
- [x] Smoke-test the sdist: extract it, cd in, cabal build with a separate cabal store (2026-04-17; built cleanly from Hackage with a throwaway cabal.project in /tmp/hw-kafka-streamly-sdist-test/hw-kafka-streamly-0.1.0.0 and `--store-dir=/tmp/cabal-store-sdist-test`)
- [x] Upload candidate to Hackage: cabal upload --publish=false (2026-04-17; sdist + docs uploaded to https://hackage.haskell.org/package/hw-kafka-streamly-0.1.0.0/candidate)
- [ ] Verify the candidate page renders correctly on hackage.haskell.org (awaiting user eyeball check)
- [ ] Promote candidate to full release: cabal upload --publish (awaiting user go-ahead after candidate review)
- [ ] Tag git: git tag -a v0.1.0.0 -m "Release 0.1.0.0" && git push origin v0.1.0.0 (awaiting user go-ahead post-publish)
- [ ] Update CHANGELOG.md: replace "0.1.0.0 (unreleased)" with "0.1.0.0 (<release date>)"
- [ ] Mark the MasterPlan complete and fill in Outcomes & Retrospective
- [ ] Commit with ExecPlan + MasterPlan + Intention trailers


## Surprises & Discoveries

- 2026-04-17 — Hackage state at plan start:
    - `streamly-core`: latest published is `0.3.0` (0.4 not on Hackage).
    - `streamly`: latest published is `0.11.0`, which pins `streamly-core == 0.3.0`.
  Confirmed via `cabal info streamly-core` / `cabal info streamly` after
  `cabal update` (index-state 2026-04-17T21:51:44Z).

- 2026-04-17 — All streamly APIs used by `hw-kafka-streamly` are already
  available in `streamly-core-0.3.0`, so the "widen" strategy requires no
  source-code changes. Verified by grepping the 0.3.0 module exports fetched
  via `cabal get streamly-core-0.3.0`:
    - `Streamly.Data.Stream.hs` exports `unfoldrM`, `fromPure`, `scanl`,
      `mapMaybeM`, `filter`, `catMaybes`, `append`, `bracketIO`.
    - `Streamly.Data.Scanl.hs` exports `mkScanl`.
    - `Streamly.Data.Fold.hs` exports `foldlM'`.
  `streamly-core-0.3.0` allows `base < 4.23`, so the GHC 9.12 `base-4.21`
  we ship with is in range.

- 2026-04-17 — The sdist smoke test requires its own `cabal.project` because
  the extracted tarball has none. A minimal file with
  `packages: .`, `with-compiler: ghc-9.12.2`, and the same `allow-newer`
  stanza as the repo root's `cabal.project` was enough; the isolated store
  at `/tmp/cabal-store-sdist-test` solved `streamly-core-0.3.0`,
  `hw-kafka-client-5.3.0`, and all transitive deps purely from Hackage and
  compiled the library successfully.

- 2026-04-17 — `cabal haddock --haddock-for-hackage` emits two cosmetic
  warnings that do not fail the build and do not need fixing for 0.1.0.0:
    - "`KafkaError` is ambiguous" (defined twice in `Kafka.Types`; Haddock
      defaults to the first).
    - "could not find link destinations for `Kafka.Types.Rep_BatchSize`"
      (an internal GHC.Generics rep name, not a public identifier).
  Haddock coverage is 100% on all three public modules.


## Decision Log

- Decision: Upload as a Hackage candidate first (`cabal upload` without `--publish`), not as a published release.
  Rationale: candidates are revisable; published releases are not. If the rendered page looks wrong or Haddock fails on the Hackage builder, a candidate can be replaced. After 24 hours of no issues, promote to a full release with `cabal upload --publish`.
  Date: 2026-04-17

- Decision: The bound strategy (wait / widen / vendor) is deferred to when this plan begins.
  Rationale: Hackage availability of `streamly-core-0.4` and `streamly-0.12` may change between now and when EP-6 starts. Re-check at the start of the plan and choose.
  Date: 2026-04-17

- Decision: Chose **widen** as the bound strategy. Drop `hw-kafka-streamly.cabal`'s
  `streamly-core >= 0.4 && < 0.5` to `streamly-core >= 0.3 && < 0.5` (leaving
  room for 0.4 once Composewell publishes it), and remove the
  `optional-packages:` block from `cabal.project` so the build resolves from
  Hackage alone.
  Rationale: `streamly-core-0.4` / `streamly-0.12` are still unpublished, and
  waiting is not an option — the user explicitly requested that we target
  published versions. Vendoring is out of scope for 0.1.0.0. All APIs used by
  `hw-kafka-streamly` (see Surprises & Discoveries) are already present in
  `streamly-core-0.3.0`, so no source edits are needed.
  Date: 2026-04-17


## Outcomes & Retrospective

(To be filled during and after implementation.)


## Context and Orientation

### The upstream-dependency problem

As of 2026-04-17:

- Hackage has `streamly-core` up to `0.3.0`.
- Hackage has `streamly` up to `0.11.x` (per `cabal info streamly`).
- The project's `hw-kafka-streamly/hw-kafka-streamly.cabal` has `streamly-core >= 0.4 && < 0.5`.
- The project's `cabal.project` has an `optional-packages:` block pointing to a local clone at `/Users/shinzui/Keikaku/hub/haskell/streamly-project/` which provides `streamly-core-0.4.0` and `streamly-0.12.0` from source.

An external user running `cabal install hw-kafka-streamly` today cannot solve: Hackage has no `streamly-core-0.4`. This plan resolves that.

### Bound strategies

Three strategies are on the table. Inspect current Hackage state at plan start to decide.

**Wait.** If `streamly-core-0.4` and `streamly-0.12` have been published to Hackage by the time this plan begins, the resolution is trivial: remove the `optional-packages:` block from `cabal.project`, run `cabal update && cabal build all`, and proceed to sdist.

**Widen.** If upstream has not published 0.4 but `streamly-core-0.3.x` is API-compatible for our usage (check: do we use `Scanl.mkScanl`, `Stream.unfoldrM`, `Stream.bracketIO`, `Stream.append`, `Stream.fromPure`, `Stream.catMaybes`, `Fold.foldlM'` in their 0.3 forms?), widen the bound in the cabal file to `streamly-core >= 0.3 && < 0.5` and verify the build solves against Hackage 0.3. This requires adjusting any code that relies on 0.4-specific behavior.

**Vendor.** Worst case, ship the needed streamly-core 0.4 source alongside `hw-kafka-streamly` in the sdist under a vendored path, or publish our own namespaced fork to Hackage. This is the heaviest option and should be avoided — if reached, reconsider whether 0.1.0.0 is premature.

### The cabal.project layout

Currently:

    packages:
      hw-kafka-streamly
      hw-kafka-streamly-jitsurei

    optional-packages:
      /Users/shinzui/Keikaku/hub/haskell/streamly-project/streamly/core/streamly-core.cabal
      /Users/shinzui/Keikaku/hub/haskell/streamly-project/streamly/streamly.cabal

    with-compiler: ghc-9.12.2

    test-show-details: direct

    allow-newer:
      *:time, *:containers, *:template-haskell, *:text, *:bytestring,
      *:base, *:ghc-prim, *:deepseq, *:filepath

The `optional-packages` block makes local development easy but makes Hackage upload impossible unless removed or conditional. For the sdist build, a separate `cabal.project.hackage` file without the `optional-packages` block, or removing the block entirely before sdist, is required.

### The sdist path

`cabal sdist hw-kafka-streamly` produces a tarball at `dist-newstyle/sdist/hw-kafka-streamly-0.1.0.0.tar.gz`. The tarball contents should include:

- `hw-kafka-streamly.cabal`
- `LICENSE`
- `README.md` (via `extra-doc-files`)
- `CHANGELOG.md` (via `extra-doc-files`)
- `src/Kafka/Streamly/Source.hs`
- `src/Kafka/Streamly/Sink.hs`
- `src/Kafka/Streamly/Combinators.hs`
- `test/Main.hs` and `test/Kafka/Streamly/*.hs` (from EP-4)

It should NOT include the jitsurei package, `docs/`, `flake.nix`, `Justfile`, or anything else outside `hw-kafka-streamly/`.

### Haddock for Hackage

`cabal haddock hw-kafka-streamly --haddock-for-hackage` produces a tarball at `dist-newstyle/hw-kafka-streamly-0.1.0.0-docs.tar.gz` suitable for upload via `cabal upload --documentation`. Hackage also re-builds docs server-side, so this step is belt-and-braces.

### Uploading

`cabal upload dist-newstyle/sdist/hw-kafka-streamly-0.1.0.0.tar.gz` uploads as a candidate by default in modern cabal (the `--publish` flag is required to fully release).

An authenticated Hackage account is required. `~/.cabal/config` stores the username and a password/token. If not configured, `cabal upload` will prompt.


## Plan of Work

Five phases: check state, apply bounds, build and package, smoke-test, upload and tag.


### 1. Check current Hackage state

Run from repository root:

    cabal update
    cabal info streamly-core | head -5
    cabal info streamly      | head -5

Record the highest published versions in the Surprises & Discoveries section.


### 2. Decide and apply the bound strategy

Record the decision in this plan's Decision Log.

If **wait** is chosen: delete the `optional-packages:` block in `cabal.project` (or comment it out with a note explaining it can be restored for local dev if streamly moves ahead of Hackage again).

If **widen** is chosen: edit `hw-kafka-streamly/hw-kafka-streamly.cabal`'s library stanza to `streamly-core >= 0.3 && < 0.5`. If source code uses APIs not in 0.3, fix or fall back. The current code uses: `Stream.unfoldrM`, `Stream.bracketIO`, `Stream.append`, `Stream.fromPure`, `Stream.catMaybes`, `Stream.filter`, `Stream.mapMaybeM`, `Stream.scanl`, `Scanl.mkScanl`, `Fold.foldlM'`. Verify each exists in `streamly-core-0.3.0` by reading the Hackage Haddock or the 0.3.0 source. If `Scanl.mkScanl` is 0.4-only (it may be — `Scanl` was a newer module), either port that code to a different primitive or wait.

If **vendor** is chosen: refactor the sdist to include vendored sources. This is out-of-band for 0.1.0.0 and likely means revisiting this plan entirely.


### 3. Verify the build and produce sdist + Haddock

After applying bounds:

    cabal update
    cabal build all

Expected: clean build with no solver errors.

    cabal sdist hw-kafka-streamly

Expected output:

    Wrote tarball sdist to
    /Users/shinzui/Keikaku/bokuno/hw-kafka-streamly/dist-newstyle/sdist/hw-kafka-streamly-0.1.0.0.tar.gz

Inspect the tarball:

    tar tzf dist-newstyle/sdist/hw-kafka-streamly-0.1.0.0.tar.gz

Verify it contains `README.md`, `CHANGELOG.md`, the three source modules, the test tree from EP-4, and no extraneous files.

Build Haddock for Hackage:

    cabal haddock hw-kafka-streamly --haddock-for-hackage

Expected output ending with a path to a `-docs.tar.gz` file.


### 4. Smoke-test the sdist in isolation

In a scratch directory, extract and build:

    mkdir -p /tmp/hw-kafka-streamly-sdist-test
    cd /tmp/hw-kafka-streamly-sdist-test
    tar xzf /Users/shinzui/Keikaku/bokuno/hw-kafka-streamly/dist-newstyle/sdist/hw-kafka-streamly-0.1.0.0.tar.gz
    cd hw-kafka-streamly-0.1.0.0
    cabal build --store-dir=/tmp/cabal-store-sdist-test

This uses a fresh cabal store so no cached local builds hide a solver failure. The build must succeed using only Hackage (plus `rdkafka` system library for hw-kafka-client).

If this step fails, the sdist is not viable. Diagnose (likely: a missing file in `extra-source-files` or an unresolvable dependency) and iterate.


### 5. Upload and tag

Upload candidate:

    cd /Users/shinzui/Keikaku/bokuno/hw-kafka-streamly
    cabal upload dist-newstyle/sdist/hw-kafka-streamly-0.1.0.0.tar.gz

Expected: URL of the candidate page on `hackage.haskell.org/package/hw-kafka-streamly-0.1.0.0/candidate`.

Upload docs:

    cabal upload --documentation dist-newstyle/hw-kafka-streamly-0.1.0.0-docs.tar.gz

Open the candidate URL in a browser. Verify: rendered README, synopsis, module list, link to bug tracker, link to source repository. If anything is wrong, iterate on EP-2 or this plan and re-upload (candidates can be replaced).

Once satisfied, promote to full release:

    cabal upload --publish dist-newstyle/sdist/hw-kafka-streamly-0.1.0.0.tar.gz
    cabal upload --publish --documentation dist-newstyle/hw-kafka-streamly-0.1.0.0-docs.tar.gz

Update CHANGELOG:

Replace `## 0.1.0.0 (unreleased)` with `## 0.1.0.0 — <YYYY-MM-DD>` where `<YYYY-MM-DD>` is today's date.

Tag git:

    git tag -a v0.1.0.0 -m "Release 0.1.0.0"
    git push origin v0.1.0.0

Do not push the tag before the user has approved the release — this is a user-visible action and deserves confirmation. Ask the user before running the final publish and tag.


## Concrete Steps

All commands run from the repository root `/Users/shinzui/Keikaku/bokuno/hw-kafka-streamly` unless noted.

Step 1. Refresh the package index and inspect upstream versions:

    cabal update
    cabal info streamly-core 2>&1 | head -5
    cabal info streamly      2>&1 | head -5

Record findings in Surprises & Discoveries.

Step 2. Decide strategy in Decision Log. Apply edits to `cabal.project` and/or `hw-kafka-streamly/hw-kafka-streamly.cabal`.

Step 3. Build to confirm the new bounds solve:

    cabal build all

Step 4. Produce sdist and Haddock:

    cabal sdist hw-kafka-streamly
    cabal haddock hw-kafka-streamly --haddock-for-hackage

Step 5. Inspect sdist contents:

    tar tzf dist-newstyle/sdist/hw-kafka-streamly-0.1.0.0.tar.gz

Step 6. Smoke-test in a scratch dir:

    mkdir -p /tmp/hw-kafka-streamly-sdist-test && cd /tmp/hw-kafka-streamly-sdist-test
    tar xzf /Users/shinzui/Keikaku/bokuno/hw-kafka-streamly/dist-newstyle/sdist/hw-kafka-streamly-0.1.0.0.tar.gz
    cd hw-kafka-streamly-0.1.0.0
    cabal build --store-dir=/tmp/cabal-store-sdist-test

Step 7. Confirm with the user before uploading. Ask explicitly: "Ready to upload `hw-kafka-streamly-0.1.0.0` to Hackage as a candidate?"

Step 8. Upload candidate and docs:

    cd /Users/shinzui/Keikaku/bokuno/hw-kafka-streamly
    cabal upload dist-newstyle/sdist/hw-kafka-streamly-0.1.0.0.tar.gz
    cabal upload --documentation dist-newstyle/hw-kafka-streamly-0.1.0.0-docs.tar.gz

Step 9. Verify the candidate page. Report the URL to the user.

Step 10. After user approval, publish:

    cabal upload --publish dist-newstyle/sdist/hw-kafka-streamly-0.1.0.0.tar.gz
    cabal upload --publish --documentation dist-newstyle/hw-kafka-streamly-0.1.0.0-docs.tar.gz

Step 11. Update CHANGELOG:

    # edit hw-kafka-streamly/CHANGELOG.md, replace (unreleased) with today's date

Step 12. Confirm with user before tagging, then:

    git tag -a v0.1.0.0 -m "Release 0.1.0.0"
    git push origin v0.1.0.0

Step 13. Commit the CHANGELOG date update:

    git add hw-kafka-streamly/CHANGELOG.md
    git commit -m "$(cat <<'EOF'
    Mark 0.1.0.0 released in CHANGELOG

    MasterPlan: docs/masterplans/1-release-hw-kafka-streamly-0.1.0.0.md
    ExecPlan: docs/plans/6-upstream-dependencies-and-hackage-release.md
    Intention: intention_01knbcpkxqemdaawn3zzs822f8
    EOF
    )"

Step 14. Update the MasterPlan's Outcomes & Retrospective section and the Exec-Plan Registry (mark EP-6 Complete).


## Validation and Acceptance

Acceptance is:

1. `cabal update && cabal build all` succeeds from a clean state with no `optional-packages` local overrides (or with documented, clearly-commented overrides for local development only).

2. `cabal sdist hw-kafka-streamly` produces `dist-newstyle/sdist/hw-kafka-streamly-0.1.0.0.tar.gz`.

3. The sdist contains `README.md` and `CHANGELOG.md` (`tar tzf ...  | grep -E 'README|CHANGELOG'` returns at least two matches).

4. The sdist builds cleanly in a scratch directory using a fresh cabal store: `cabal build --store-dir=/tmp/cabal-store-sdist-test` succeeds.

5. The Hackage candidate page at `https://hackage.haskell.org/package/hw-kafka-streamly-0.1.0.0/candidate` loads and shows the rendered README, module list, and source-repository link. (This is a human-eye check.)

6. After publish, `cabal install hw-kafka-streamly --only-dependencies` from a scratch directory succeeds. (Depends on user confirming final publish.)

7. `git tag | grep v0.1.0.0` returns `v0.1.0.0`.

8. `hw-kafka-streamly/CHANGELOG.md` contains `## 0.1.0.0 — <date>` (no `(unreleased)`).

9. The MasterPlan's Exec-Plan Registry shows EP-6 as Complete.


## Idempotence and Recovery

Uploading a candidate is idempotent — re-uploading replaces the candidate. Publishing a release is NOT idempotent. Once `cabal upload --publish` goes through, that version number is permanently taken on Hackage. If 0.1.0.0 is published in error, the next attempt must be 0.1.0.1.

If the sdist smoke test fails, iterate on cabal metadata (missing `extra-source-files`, unused `main-is`, etc.) and re-sdist. This is safe to do as many times as needed.

If the Hackage upload fails with an auth error, verify `~/.cabal/config` has `username:` and `password:`/`password-command:` set, or use `cabal upload` interactively.

If the git tag is pushed in error, remove it with `git tag -d v0.1.0.0 && git push origin :refs/tags/v0.1.0.0`. This is destructive — confirm with the user first.


## Interfaces and Dependencies

No new Haskell code. This plan's edits are to cabal metadata, `cabal.project`, and `CHANGELOG.md` only.

Tools required:

- `cabal-install` (bundled in the Nix dev shell).
- A Hackage account for the maintainer (Nadeem Bitar).
- `git` with push access to `origin`.

External service dependencies:

- `hackage.haskell.org` must be up.
- Whatever published state `streamly-core` and `streamly` are in at plan-start determines the bound strategy.
