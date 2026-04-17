# Release packaging and metadata

MasterPlan: docs/masterplans/1-release-hw-kafka-streamly-0.1.0.0.md

Intention: intention_01knbcpkxqemdaawn3zzs822f8

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

After this plan is complete, the `hw-kafka-streamly` package has everything Hackage expects on a package page: a README that renders as the package description, a CHANGELOG that lets users see what 0.1.0.0 ships, and complete cabal metadata (homepage, bug tracker, source repo, tested-with, copyright). The `bifunctors` dependency — currently declared but never imported — is removed so `cabal build --ghc-options=-Wunused-packages` is clean. The jitsurei package's synopsis clearly states it is the cookbook-examples package, not a library to depend on.

A Haskell developer visiting the future Hackage page will see a rendered README explaining the library in under a minute, click through to the GitHub repository via the `homepage` link, and find the installation snippet and a minimal usage example. `cabal check` passes with zero errors and zero warnings against the edited cabal file.

This plan makes no Haskell source code changes and no API changes. It touches the cabal file, adds two new root-level Markdown files, and adjusts the jitsurei cabal synopsis. It is entirely safe to merge before any of the other release-readiness work.


## Progress

- [x] Read the current state: confirm no README.md, no CHANGELOG.md, current cabal metadata (2026-04-17)
- [x] Add hw-kafka-streamly/README.md with overview, install snippet, minimal usage example (2026-04-17)
- [x] Add hw-kafka-streamly/CHANGELOG.md with 0.1.0.0 (unreleased) section (2026-04-17)
- [x] Edit hw-kafka-streamly/hw-kafka-streamly.cabal: add homepage, bug-reports, source-repository head, copyright, tested-with, extra-doc-files (2026-04-17)
- [x] Edit hw-kafka-streamly/hw-kafka-streamly.cabal: remove bifunctors from build-depends (2026-04-17)
- [x] Edit hw-kafka-streamly-jitsurei/hw-kafka-streamly-jitsurei.cabal: tighten synopsis (2026-04-17)
- [x] Verify cabal build all succeeds (2026-04-17)
- [x] Verify cabal build hw-kafka-streamly --ghc-options=-Wunused-packages produces no warnings (2026-04-17)
- [x] Verify cd hw-kafka-streamly && cabal check passes with zero errors and zero warnings (2026-04-17)
- [ ] Commit all changes with ExecPlan + MasterPlan + Intention trailers


## Surprises & Discoveries

- 2026-04-17: `cabal build hw-kafka-streamly --ghc-options="-Wunused-packages" -fforce-recomp` reported "Up to date" instead of recompiling — cabal does not treat the additional `--ghc-options` value as a configuration change once the package has already been built. Workaround: delete `dist-newstyle/build/.../hw-kafka-streamly-0.1.0.0` and rebuild. Result was clean (zero `-Wunused-packages` warnings), confirming the `bifunctors` removal is correct.


## Decision Log

- Decision: Place README.md and CHANGELOG.md inside `hw-kafka-streamly/` (alongside the `.cabal` file), not at the repository root.
  Rationale: Hackage's sdist includes files listed in `extra-doc-files` relative to the package directory. The cabal file is at `hw-kafka-streamly/hw-kafka-streamly.cabal`, so `extra-doc-files: README.md CHANGELOG.md` resolves to `hw-kafka-streamly/README.md` and `hw-kafka-streamly/CHANGELOG.md`. Putting them at the repository root would require absolute or relative paths that don't ship cleanly with the sdist.
  Date: 2026-04-17


## Outcomes & Retrospective

Implementation completed 2026-04-17.

- README.md (~110 lines) and CHANGELOG.md (Keep-a-Changelog 0.1.0.0 unreleased entry) live in `hw-kafka-streamly/` and are wired through `extra-doc-files`.
- Library cabal gains `copyright`, `homepage`, `bug-reports`, `tested-with`, `extra-doc-files`, and a `source-repository head` stanza pointing at `https://github.com/shinzui/hw-kafka-streamly.git`.
- `bifunctors` dependency removed; clean rebuild of `hw-kafka-streamly` with `-Wunused-packages` produces zero warnings.
- `cabal check` from `hw-kafka-streamly/`: "No errors or warnings could be found in the package."
- Jitsurei synopsis now reads `Cookbook examples for hw-kafka-streamly (not intended for publication)`.
- No Haskell source touched; no API change; safe to merge ahead of EP-3..EP-6.


## Context and Orientation

The repository is a multi-package Haskell project at `/Users/shinzui/Keikaku/bokuno/hw-kafka-streamly`. Its layout:

    hw-kafka-streamly/                   -- the library package (published to Hackage)
      hw-kafka-streamly.cabal
      LICENSE
      src/Kafka/Streamly/
        Source.hs
        Sink.hs
        Combinators.hs
    hw-kafka-streamly-jitsurei/          -- the cookbook-examples package (not published)
      hw-kafka-streamly-jitsurei.cabal
      LICENSE
      app/                               -- seven executable Main modules
      src/HwKafkaStreamly/Jitsurei/Config.hs
    cabal.project                        -- lists both packages plus optional-packages for streamly
    flake.nix                            -- Nix dev shell including rdkafka
    process-compose.yaml                 -- launches a Redpanda broker for local testing
    Justfile                             -- developer convenience recipes
    docs/plans/1-streamly-bindings-for-hw-kafka-client.md  -- the original design plan
    docs/masterplans/1-release-hw-kafka-streamly-0.1.0.0.md -- the master plan this plan is a child of

The library package `hw-kafka-streamly` exposes three modules: `Kafka.Streamly.Source`, `Kafka.Streamly.Sink`, `Kafka.Streamly.Combinators`. It depends on `hw-kafka-client` (a Haskell binding to librdkafka, the C library that talks to Apache Kafka) and `streamly-core` (a streaming data library). The library provides Streamly `Stream` sources for consuming Kafka messages and Streamly `Fold` sinks for producing them.

**Current cabal metadata**, from `hw-kafka-streamly/hw-kafka-streamly.cabal`:

    cabal-version: 3.4
    name:          hw-kafka-streamly
    version:       0.1.0.0
    synopsis:      Streamly bindings for hw-kafka-client
    description:
      Streamly streaming integration for hw-kafka-client, a Haskell
      binding to Apache Kafka via librdkafka. Provides composable stream
      sources for consuming and fold-based sinks for producing, with safe
      resource management.

    license:       MIT
    license-file:  LICENSE
    author:        Nadeem Bitar
    maintainer:    Nadeem Bitar
    category:      Kafka
    build-type:    Simple

Missing fields a Hackage package page expects: `homepage`, `bug-reports`, `copyright`, `tested-with`, `extra-doc-files`, and a `source-repository head` stanza.

**Current build-depends on the library stanza**:

    build-depends:
      , base             >=4.21 && <5
      , bifunctors       >=5.6  && <6
      , bytestring       >=0.11 && <0.13
      , exceptions       >=0.10 && <1
      , hw-kafka-client  >=5.3  && <6
      , streamly-core    >=0.4  && <0.5

The `bifunctors` line is confirmed unused: running `cabal build hw-kafka-streamly --ghc-options="-Wunused-packages"` emits

    warning: [GHC-42258] [-Wunused-packages]
        The following packages were specified via -package or -package-id flags,
        but were not needed for compilation:
          - bifunctors-5.6.3

The three source modules import `Data.Bifunctor (Bifunctor, bimap, first)` and `Data.Bitraversable (Bitraversable, bisequenceA, bitraverse)` — these classes live in `base` (since base-4.8 and base-4.10 respectively), not in the separate `bifunctors` package on Hackage (which provides additional classes like `Biapplicative`). Removing the line is safe.

**The jitsurei package's current synopsis**, from `hw-kafka-streamly-jitsurei/hw-kafka-streamly-jitsurei.cabal`:

    synopsis:      Cookbook examples for hw-kafka-streamly

This is not on Hackage and should not be depended on by external users. The synopsis should make that clearer.

**GitHub remote** (needed for homepage and source-repository): run `git remote -v` from the repository root. As of 2026-04-17 the repo is at `shinzui/hw-kafka-streamly` per the project's `mori.dhall`:

    , repos =
      [ { name = "hw-kafka-streamly"
        , github = Some "shinzui/hw-kafka-streamly"

So the homepage URL is `https://github.com/shinzui/hw-kafka-streamly` and the bug tracker is `https://github.com/shinzui/hw-kafka-streamly/issues`.

**Author**: Nadeem Bitar. Copyright: 2026 Nadeem Bitar.


## Plan of Work

Four independent edits. Order does not matter, but validation (`cabal check`, `cabal build`) must happen after all of them.


### 1. Create `hw-kafka-streamly/README.md`

Write a concise package README. It must render well on both GitHub and Hackage (both use CommonMark). Target length: roughly 80 lines.

Structure: a one-line elevator pitch, a short description of what the package does, an installation snippet, a minimal consumer example, a minimal producer example, a link to the jitsurei cookbook for more patterns, a link to the original design plan, and a license line.

The README should mention: (a) that it depends on `streamly-core` 0.4 (and note that as of writing 0.4 is not yet on Hackage — see EP-5 in the MasterPlan), (b) that resource management on the consumer side comes in three tiers (`kafkaSource`, `kafkaSourceAutoClose`, `kafkaSourceNoClose`), (c) that producer sinks are Streamly `Fold`s and come with a `withKafkaProducer` bracket helper.

Keep code samples runnable: they should compile if pasted into a `main` module with the imports shown.


### 2. Create `hw-kafka-streamly/CHANGELOG.md`

Write a CHANGELOG following the Keep-a-Changelog convention. The 0.1.0.0 entry should summarize the three modules at a high level (source module with three consumer variants, error predicates and filters, value-mapping helpers; sink module with two fold variants and a bracket helper; combinators module with batching and error-throwing combinators).

Mark the version as `(unreleased)` until EP-6 tags the release and replaces `(unreleased)` with the release date.

Structure:

    # Changelog

    All notable changes to this project will be documented in this file.

    The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
    and this project adheres to [Haskell Package Versioning Policy](https://pvp.haskell.org/).

    ## 0.1.0.0 (unreleased)

    Initial release.

    ### Added

    - `Kafka.Streamly.Source` — three consumer stream variants ...
    - `Kafka.Streamly.Sink` — two producer fold variants ...
    - `Kafka.Streamly.Combinators` — batching and error-throwing combinators ...


### 3. Edit `hw-kafka-streamly/hw-kafka-streamly.cabal` — add metadata and drop unused dep

Add the following fields to the top stanza, after `build-type: Simple`:

    homepage:      https://github.com/shinzui/hw-kafka-streamly
    bug-reports:   https://github.com/shinzui/hw-kafka-streamly/issues
    copyright:     2026 Nadeem Bitar
    tested-with:   GHC == 9.12.2
    extra-doc-files:
      CHANGELOG.md
      README.md

Add a `source-repository head` stanza after the top-stanza metadata and before `common warnings`:

    source-repository head
      type:     git
      location: https://github.com/shinzui/hw-kafka-streamly.git

In the `library` stanza's `build-depends` block, remove the `bifunctors` line. The block should read:

    build-depends:
      , base             >=4.21 && <5
      , bytestring       >=0.11 && <0.13
      , exceptions       >=0.10 && <1
      , hw-kafka-client  >=5.3  && <6
      , streamly-core    >=0.4  && <0.5


### 4. Edit `hw-kafka-streamly-jitsurei/hw-kafka-streamly-jitsurei.cabal` — clarify synopsis

Replace:

    synopsis:      Cookbook examples for hw-kafka-streamly

With:

    synopsis:      Cookbook examples for hw-kafka-streamly (not intended for publication)

Nothing else in that file needs changing.


## Concrete Steps

All commands run from the repository root `/Users/shinzui/Keikaku/bokuno/hw-kafka-streamly` unless noted.

Step 1. Confirm there is no existing README or CHANGELOG anywhere in the package directories:

    find hw-kafka-streamly hw-kafka-streamly-jitsurei -maxdepth 2 -type f \( -iname "README*" -o -iname "CHANGELOG*" \)

Expected: no output.

Step 2. Create `hw-kafka-streamly/README.md`. Use the structure described in the Plan of Work section above.

Step 3. Create `hw-kafka-streamly/CHANGELOG.md`. Use the structure described above.

Step 4. Edit `hw-kafka-streamly/hw-kafka-streamly.cabal` per the Plan of Work.

Step 5. Edit `hw-kafka-streamly-jitsurei/hw-kafka-streamly-jitsurei.cabal` per the Plan of Work.

Step 6. Verify the build still works:

    cabal build all

Expected: all packages build with no warnings, no errors.

Step 7. Verify the `bifunctors` removal is correct:

    cabal build hw-kafka-streamly --ghc-options="-Wunused-packages" -fforce-recomp

Expected: no `-Wunused-packages` warning for hw-kafka-streamly. (You may see one for streamly-core's own build — that is upstream's problem, not ours.)

Step 8. Run cabal check from the library directory:

    cd hw-kafka-streamly && cabal check

Expected:

    No errors or warnings could be found in the package.

Step 9. Stage and commit:

    git add hw-kafka-streamly/README.md \
            hw-kafka-streamly/CHANGELOG.md \
            hw-kafka-streamly/hw-kafka-streamly.cabal \
            hw-kafka-streamly-jitsurei/hw-kafka-streamly-jitsurei.cabal
    git commit -m "$(cat <<'EOF'
    Add release packaging and metadata for hw-kafka-streamly

    - Add README.md and CHANGELOG.md inside the library package so
      extra-doc-files picks them up in the Hackage sdist.
    - Add homepage, bug-reports, copyright, tested-with, extra-doc-files,
      and a source-repository head stanza to the cabal file.
    - Remove unused bifunctors build-depends (Data.Bifunctor and
      Data.Bitraversable live in base).
    - Clarify jitsurei package synopsis.

    MasterPlan: docs/masterplans/1-release-hw-kafka-streamly-0.1.0.0.md
    ExecPlan: docs/plans/2-release-packaging-and-metadata.md
    Intention: intention_01knbcpkxqemdaawn3zzs822f8
    EOF
    )"


## Validation and Acceptance

Acceptance is:

1. The three commands all pass:

       cabal build all
       cabal build hw-kafka-streamly --ghc-options="-Wunused-packages" -fforce-recomp
       cd hw-kafka-streamly && cabal check

   with no warnings and no errors (`-Wunused-packages` produces zero warnings for the hw-kafka-streamly package specifically).

2. `ls hw-kafka-streamly/README.md hw-kafka-streamly/CHANGELOG.md` both exist.

3. `grep -c "^homepage:\|^bug-reports:\|^source-repository\|^tested-with:\|^copyright:\|^extra-doc-files:" hw-kafka-streamly/hw-kafka-streamly.cabal` returns at least 6.

4. `grep "bifunctors" hw-kafka-streamly/hw-kafka-streamly.cabal` returns nothing.

5. The git log shows a commit with the `ExecPlan:`, `MasterPlan:`, and `Intention:` trailers.


## Idempotence and Recovery

All edits are idempotent under content inspection. If `cabal check` surfaces a warning, fix it and re-run. If a commit needs amending, prefer a follow-up commit (per the repository's git safety protocol: do not amend).

If the README or CHANGELOG file already exists (because this plan is being re-run), inspect the existing content before overwriting — the prior version may be from a previous attempt and can be kept.


## Interfaces and Dependencies

No new Haskell interfaces are defined or changed by this plan. The only dependency change is the removal of `bifunctors` from `hw-kafka-streamly/hw-kafka-streamly.cabal`'s `library` stanza — that dependency was unused.
