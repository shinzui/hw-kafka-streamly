# Jitsurei hardening and live validation

MasterPlan: docs/masterplans/1-release-hw-kafka-streamly-0.1.0.0.md

Intention: intention_01knbcpkxqemdaawn3zzs822f8

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

After this plan is complete, every one of the seven cookbook executables in the `hw-kafka-streamly-jitsurei` package has been run end-to-end against a local Redpanda broker started via the repository's `process-compose.yaml`, and the observed transcripts are captured in a validation log at `docs/validation/0.1.0.0-jitsurei.md`. The one subtly-wrong example (`ConsumeProduce.hs`, which currently drops all errors — including fatal ones — via `mapMaybe`) is corrected to use the documented error-handling idiom. The concurrent example (`ConcurrentConsume.hs`) gains a brief comment about `parMapM`'s effect on message ordering so readers do not assume FIFO.

The final item left unchecked in the original design plan at `docs/plans/1-streamly-bindings-for-hw-kafka-client.md` — "Validate examples against running Redpanda (manual test)" — is marked complete. A contributor running `just process-up` followed by `cabal run streamly-producer && cabal run streamly-consumer` sees messages flow and the consumer print them.

"Jitsurei" (実例) is the name the user uses for cookbook/sample-code packages. In this repo it is the package at `hw-kafka-streamly-jitsurei/` with seven executables in `app/`. This plan touches that package, the `process-compose.yaml` broker harness, and a new `docs/validation/` directory; it does not touch the library package's source.


## Progress

- [x] Read ConsumeProduce.hs and identify the mapMaybe that drops fatal errors (2026-04-17)
- [x] Replace the error-dropping mapMaybe with skipNonFatal + throwLeft (or documented equivalent) (2026-04-17)
- [x] Add an ordering comment to ConcurrentConsume.hs (2026-04-17)
- [x] Verify cabal build all still succeeds after the jitsurei edits (2026-04-17)
- [x] Start Redpanda via just process-up (or process-compose up) (2026-04-17)
- [x] Create the input topic: just create-topic (actual recipe name; plan originally said kafka-create-topic) (2026-04-17)
- [x] Create the output topic: just create-output-topic (2026-04-17)
- [x] Run streamly-producer and capture output (2026-04-17)
- [x] Run streamly-consumer and capture output (2026-04-17)
- [x] Run error-handling and capture output (2026-04-17)
- [x] Run transform-pipeline and capture output (2026-04-17)
- [x] Run batch-sink and capture output (2026-04-17)
- [x] Run consume-produce and capture output (2026-04-17)
- [x] Run concurrent-consume and capture output (2026-04-17)
- [x] Save transcripts to docs/validation/0.1.0.0-jitsurei.md (2026-04-17)
- [x] Mark the Redpanda validation item complete in docs/plans/1-streamly-bindings-for-hw-kafka-client.md (2026-04-17)
- [x] Stop Redpanda: rpk container purge (process-compose socket was gone; used rpk directly) (2026-04-17)
- [ ] Commit with ExecPlan + MasterPlan + Intention trailers


## Surprises & Discoveries

- The Justfile recipe names differ from what this plan originally referenced.
  The plan says `just kafka-create-topic`; the actual recipe is `just
  create-topic`. A separate recipe `just create-output-topic` exists for the
  ETL output topic. (Observed 2026-04-17 via `just --list`.)

- `just process-up` returns exit 0 shortly after starting Redpanda, rather
  than blocking for the lifetime of the broker. The underlying `rpk
  container start` command returns once the Docker container is running, so
  process-compose notices its single process has "finished" and exits. The
  broker continues running independently; `just process-down` still works to
  stop it via `rpk container purge`. This meant running `just process-up` in
  the foreground was sufficient — no background-process juggling needed.
  (Observed 2026-04-17.)

- librdkafka prints `Connect to ipv6#[::1]:9092 failed: Connection refused`
  as an ERROR-level log on every consumer/producer start. `localhost`
  resolves to both `::1` and `127.0.0.1`; the IPv6 attempt fails because
  Redpanda binds only IPv4, and librdkafka falls back silently. No user
  impact. Left out of the validation transcripts for readability.

- Both `jitsurei-topic` and `jitsurei-streamly-output` existed from prior
  runs when I first queried the cluster. Recreated them cleanly via
  `rpk topic delete` + `just create-topic` + `just create-output-topic`
  before capturing transcripts.

- `just process-down` failed with "no such file or directory" on the
  unix socket because process-compose had already exited earlier (see
  previous discovery). Stopped the broker directly with
  `rpk container purge`, which is what the `shutdown.command` in
  `process-compose.yaml` invokes anyway.


## Decision Log

- Decision: The fix for `ConsumeProduce.hs` uses `skipNonFatal` followed by `throwLeft` (rather than a silent `mapMaybe`).
  Rationale: `skipNonFatal` drops recoverable errors (poll timeouts, partition EOF) but keeps fatal ones, and `throwLeft` turns the remaining fatal ones into exceptions. That is the documented, idiomatic way to convert a `Stream m (Either KafkaError a)` into `Stream m a` without silently losing fatal errors. The alternative of handling fatal errors via a `catch` at the top level of `main` is also acceptable but requires more ceremony in a cookbook example.
  Date: 2026-04-17

- Decision: Validation transcripts live in `docs/validation/`, not in the cookbook package itself.
  Rationale: transcripts are time-bound artifacts tied to a specific release, not executable code. They do not belong in the `app/` directory. `docs/validation/0.1.0.0-jitsurei.md` can be archived and future releases can add their own files without cluttering the source tree.
  Date: 2026-04-17


## Outcomes & Retrospective

Completed 2026-04-17.

Acceptance criteria met:

1. `cabal build all` succeeds after the two source edits. The only warning
   is a pre-existing `directory` unused-package note in the library, not
   introduced by this plan.
2. `docs/validation/0.1.0.0-jitsurei.md` exists and covers all seven
   executables with observed transcripts.
3. All seven executables exited with code 0 during validation.
4. The "Validate examples against running Redpanda" checkbox in the
   original design plan is now `[x]`.
5. `grep "mapMaybe (either"
   hw-kafka-streamly-jitsurei/app/ConsumeProduce.hs` returns nothing —
   the silent error-drop is gone; `throwLeft` is imported and used
   in its place.
6. The new ordering comment appears above the `pipeline = ` binding in
   `ConcurrentConsume.hs`; the observed output confirms interleaving.

Retrospective notes:

- The Plan of Work's biggest leverage was in #1 (the `ConsumeProduce.hs`
  fix). Option A (`throwLeft . skipNonFatal`) turned out to be a
  two-line change and read naturally against the rest of the file.
- The validation session took ~10 minutes of wall clock once the broker
  was up, well within the plan's implicit budget. Pre-existing Docker
  state from prior work meant `just process-up` returned in seconds.
- No library bugs were discovered. The EP-3 escalation path described
  in Idempotence and Recovery was not needed.


## Context and Orientation

### The jitsurei package

`hw-kafka-streamly-jitsurei/` is a cabal package with one library (`HwKafkaStreamly.Jitsurei.Config` exporting a few default constants) and eight executables:

    app/Main.hs                    -- help banner listing the examples
    app/StreamlyProducer.hs        -- produces 5 messages to defaultTopicName
    app/StreamlyConsumer.hs        -- consumes up to 10 messages and prints them
    app/ErrorHandling.hs           -- demonstrates skipNonFatal, skipNonFatalExcept, throwLeft
    app/TransformPipeline.hs       -- demonstrates mapValue and inline fmap patterns
    app/BatchSink.hs               -- demonstrates batchByOrFlush + kafkaBatchSink
    app/ConsumeProduce.hs          -- ETL: consume -> transform -> produce
    app/ConcurrentConsume.hs       -- parMapM over the consumer stream

The library stanza exposes:

    HwKafkaStreamly.Jitsurei.Config
      defaultBrokerAddress :: BrokerAddress   -- "localhost:9092"
      defaultTimeout       :: Timeout          -- Timeout 10000
      defaultTopicName     :: TopicName        -- "jitsurei-topic"
      outputTopicName      :: TopicName        -- "jitsurei-streamly-output"

### The broker harness

`process-compose.yaml` at the repository root defines services that can be launched with `process-compose up` (or, via the Justfile, `just process-up`). One of those services is a Redpanda broker exposing Kafka protocol on `localhost:9092`. Topics are created via the Justfile recipes `kafka-create-topic` and `kafka-delete-topic`.

Read the `Justfile` to see the exact recipe names:

    cat Justfile

and the `process-compose.yaml` to see service dependencies:

    cat process-compose.yaml

### The bug in ConsumeProduce.hs

At lines 74-76 of `hw-kafka-streamly-jitsurei/app/ConsumeProduce.hs`:

    Stream.mapM ( \record -> ... ) $
      Stream.mapMaybe (either (const Nothing) Just) $
        Stream.take 5 $
          skipNonFatal source

The `Stream.mapMaybe (either (const Nothing) Just)` drops every `Left` in the stream, including fatal ones. `skipNonFatal` above it removes *non-fatal* lefts, but fatal ones are then silently dropped by `mapMaybe`. A consumer that hits an authentication failure would, per this code, just stop silently and produce fewer than 5 output records without any error surfacing.

The right pattern is either `throwLeft . skipNonFatal` (fatal errors escape as exceptions) or a pattern-match on the Either that explicitly handles the fatal case and chooses whether to abort or log.

### The ConcurrentConsume ordering caveat

`hw-kafka-streamly-jitsurei/app/ConcurrentConsume.hs` uses `StreamP.parMapM` which dispatches work to multiple threads. This means the order of output lines is not the input order — `[start]` lines for record N can interleave with `[done]` lines for record N-2. The example currently prints `(maxThreads=4, maxBuffer=8)` in its banner but does not mention that ordering is no longer preserved. A one-line comment prevents confusion.

### Justfile expected recipes

Per the design plan at `docs/plans/1-streamly-bindings-for-hw-kafka-client.md`:

    groups: services (process-up, process-down), kafka (create-topic, delete-topic,
    list-topics), and build (build, clean, fmt)

Confirm by running `just --list` before starting the validation run. The actual recipe names may have minor variation (e.g., `kafka-create-topic` vs `create-topic`); use what `just --list` shows.

### Output topic

Some examples produce to `outputTopicName = "jitsurei-streamly-output"`. This topic must be pre-created for `consume-produce` and `batch-sink` to succeed (depending on broker auto-create settings; Redpanda's default is `auto.create.topics.enable=true` so this may be automatic, but verify).


## Plan of Work

Two source edits, one validation session, one new document.


### 1. Fix `ConsumeProduce.hs` to not silently drop fatal errors

File: `hw-kafka-streamly-jitsurei/app/ConsumeProduce.hs`.

Replace:

    Stream.mapMaybe (either (const Nothing) Just) $
      Stream.take 5 $
        skipNonFatal source

With either of:

Option A (throw on fatal):

    Stream.take 5 $
      throwLeft $
        skipNonFatal source

Requires importing `throwLeft` from `Kafka.Streamly.Combinators`.

Option B (explicit handling, less cookbook-y):

    -- keep the Stream.take 5 outermost; convert Either by logging and continuing
    Stream.mapM (\case
        Left err -> do putStrLn ("  Fatal error from consumer: " <> show err); error "aborting"
        Right record -> ...) $
      Stream.take 5 $
        skipNonFatal source

Prefer option A for the cookbook — it uses the library's documented idiom.


### 2. Annotate `ConcurrentConsume.hs` with an ordering note

File: `hw-kafka-streamly-jitsurei/app/ConcurrentConsume.hs`.

Immediately before the `let pipeline =` block, insert a comment:

    -- Note: parMapM dispatches work across threads, so [start] and [done]
    -- lines below will not appear in input order. This is expected.

No code change.


### 3. Run all seven executables against Redpanda and capture transcripts

This is the manual validation step. Sequence:

    just process-up                         # launches Redpanda via process-compose

Wait for the readiness probe to pass — `rpk cluster info` reports the broker.

    just kafka-create-topic                 # or the equivalent recipe; verify with just --list

(This should create `jitsurei-topic` per the default config. If a recipe for the output topic is missing and auto-create is disabled, also create `jitsurei-streamly-output`.)

For each executable, run it and save the full stdout+stderr output to a scratch file. A typical session:

    cabal run streamly-producer  2>&1 | tee /tmp/val-streamly-producer.txt
    cabal run streamly-consumer  2>&1 | tee /tmp/val-streamly-consumer.txt
    cabal run error-handling     2>&1 | tee /tmp/val-error-handling.txt
    cabal run transform-pipeline 2>&1 | tee /tmp/val-transform-pipeline.txt
    cabal run batch-sink         2>&1 | tee /tmp/val-batch-sink.txt
    cabal run consume-produce    2>&1 | tee /tmp/val-consume-produce.txt
    cabal run concurrent-consume 2>&1 | tee /tmp/val-concurrent-consume.txt

Order matters for some examples: run `streamly-producer` and `batch-sink` before the consumers so the topic has messages. Re-run the producer between consumer tests if needed to keep offsets advancing.

For each run, confirm: exit code 0, no unexpected errors in output, messages flowing as the example's banner claims.


### 4. Record transcripts in `docs/validation/0.1.0.0-jitsurei.md`

Create the file. Include:

- The date of validation.
- The Redpanda version (`rpk version`) and broker URL.
- The GHC version (`ghc --version`).
- For each of the seven executables, a short section with the command, expected behavior, and the first ~20 lines of observed output (enough to demonstrate the example worked, without dumping everything).
- A "Conclusion" section stating all seven examples function as documented.


### 5. Mark the design plan's open item complete

File: `docs/plans/1-streamly-bindings-for-hw-kafka-client.md`.

The current line 58 is:

    - [ ] Validate examples against running Redpanda (manual test)

Change to:

    - [x] Validate examples against running Redpanda (manual test) (2026-<MM>-<DD>, see docs/validation/0.1.0.0-jitsurei.md)

Fill in the real date.


## Concrete Steps

All commands run from the repository root `/Users/shinzui/Keikaku/bokuno/hw-kafka-streamly` unless noted.

Step 1. Inspect the Justfile to confirm recipe names:

    just --list

Record the observed recipes in the Surprises & Discoveries section if they differ from the ones referenced here.

Step 2. Apply edits 1 and 2 per Plan of Work.

Step 3. Verify the cookbook package still builds:

    cabal build all

Expected: clean build, no warnings.

Step 4. Start Redpanda:

    just process-up

Wait until the readiness probe succeeds (watch process-compose's output or `rpk cluster info` from another shell).

Step 5. Create topics:

    just kafka-create-topic

(If a separate recipe exists for the output topic, run it. Otherwise rely on auto-create.)

Step 6. Run the seven executables and tee their output, as listed in Plan of Work edit 3.

Step 7. Create `docs/validation/`:

    mkdir -p docs/validation

Step 8. Write `docs/validation/0.1.0.0-jitsurei.md` per Plan of Work edit 4.

Step 9. Edit `docs/plans/1-streamly-bindings-for-hw-kafka-client.md` per edit 5.

Step 10. Stop the broker:

    just process-down

Step 11. Commit:

    git add hw-kafka-streamly-jitsurei/app/ConsumeProduce.hs \
            hw-kafka-streamly-jitsurei/app/ConcurrentConsume.hs \
            docs/validation/0.1.0.0-jitsurei.md \
            docs/plans/1-streamly-bindings-for-hw-kafka-client.md
    git commit -m "$(cat <<'EOF'
    Harden jitsurei examples and validate against local Redpanda

    - Fix ConsumeProduce.hs: replace silent mapMaybe with throwLeft so
      fatal errors surface as exceptions.
    - Add ordering note to ConcurrentConsume.hs clarifying that parMapM
      does not preserve input order.
    - Validate all 7 executables against local Redpanda; transcripts at
      docs/validation/0.1.0.0-jitsurei.md.
    - Mark the "Validate examples against running Redpanda" item complete
      in the original design plan.

    MasterPlan: docs/masterplans/1-release-hw-kafka-streamly-0.1.0.0.md
    ExecPlan: docs/plans/5-jitsurei-hardening-and-validation.md
    Intention: intention_01knbcpkxqemdaawn3zzs822f8
    EOF
    )"


## Validation and Acceptance

Acceptance is:

1. `cabal build all` succeeds with no warnings after edits 1 and 2.

2. `docs/validation/0.1.0.0-jitsurei.md` exists and contains a section for each of the seven executables (`streamly-producer`, `streamly-consumer`, `error-handling`, `transform-pipeline`, `batch-sink`, `consume-produce`, `concurrent-consume`) with observed output.

3. Each of the seven executables exited with code 0 during the validation run.

4. The "Validate examples against running Redpanda" checkbox in `docs/plans/1-streamly-bindings-for-hw-kafka-client.md` is marked `[x]`.

5. `grep "mapMaybe (either" hw-kafka-streamly-jitsurei/app/ConsumeProduce.hs` returns nothing (the silent error-drop is gone).

6. `grep "parMapM" hw-kafka-streamly-jitsurei/app/ConcurrentConsume.hs | head -5` shows the code intact, and `grep -B1 "parMapM" hw-kafka-streamly-jitsurei/app/ConcurrentConsume.hs` shows the new comment above the `let pipeline =` line.


## Idempotence and Recovery

The two source edits are idempotent — re-applying is a no-op once applied.

The validation run is repeatable: re-running the executables produces different output (different offsets, different timestamps) but the same overall behavior. If a run fails because a topic does not exist or the broker is not ready, fix the prerequisite and rerun.

If `just process-up` fails, `docker ps` and `rpk container ls` can surface the underlying cause. `just process-down` is safe to run at any time to clean up.

If a specific executable fails in a way that reveals a library bug, stop here and escalate to EP-3 (the API plan owns library fixes). Record the failure in Surprises & Discoveries with the full error output.


## Interfaces and Dependencies

No new Haskell dependencies.

Requires a running Redpanda broker on `localhost:9092`. Provided by the repository's `process-compose.yaml`.

Requires `just` and `process-compose` to be on PATH. Both are provided by the Nix dev shell — run `nix develop` if missing, or install via the system package manager.

Does not edit the library under `hw-kafka-streamly/` — only the jitsurei cookbook and documentation.
