# hw-kafka-streamly

Streamly streaming integration for [`hw-kafka-client`][hw-kafka-client] — the
Haskell binding to Apache Kafka via [librdkafka][librdkafka]. Kafka consumers
become Streamly `Stream`s and producers become Streamly `Fold`s, so
message-processing pipelines compose with everything else in the Streamly
ecosystem.

The library is on Hackage:
<https://hackage.haskell.org/package/hw-kafka-streamly>.

## Repository layout

This repository contains two cabal packages plus supporting tooling:

- [`hw-kafka-streamly/`](./hw-kafka-streamly) — the published library. Its
  own [README](./hw-kafka-streamly/README.md) is what renders on Hackage and
  covers the public API.
- [`hw-kafka-streamly-jitsurei/`](./hw-kafka-streamly-jitsurei) — end-to-end
  runnable cookbook examples (not published). Seven executables covering a
  basic consumer and producer, transform pipelines, batching folds,
  error handling, consume-then-produce, and concurrent consumers.
- [`docs/`](./docs) — design documents: a MasterPlan for the 0.1.0.0
  release and per-workstream ExecPlans.
- `flake.nix`, `process-compose.yaml`, `Justfile` — a Nix dev shell with
  GHC 9.12, `cabal-install`, `rpk`, and a `process-compose` recipe to start
  a local Redpanda broker for running the jitsurei cookbook end to end.

## Getting started

### Using the library

    cabal install --lib hw-kafka-streamly

Then read [`hw-kafka-streamly/README.md`](./hw-kafka-streamly/README.md) for
a worked consume-and-produce example, and the Haddock on Hackage for the
full API.

### Hacking on this repo

If you have [Nix][nix] with flakes enabled:

    nix develop
    cabal build all
    cabal test hw-kafka-streamly

Without Nix: you need GHC 9.12.2, a recent `cabal-install`, and the
`librdkafka` C library installed (Homebrew: `brew install librdkafka`;
Debian/Ubuntu: `apt install librdkafka-dev`).

### Running the cookbook against a local broker

From inside the Nix dev shell:

    just process-up        # starts Redpanda via rpk container
    just create-topic      # creates jitsurei-topic
    cabal run streamly-producer
    cabal run streamly-consumer
    just process-down      # tears down Redpanda

`just --list` shows the full menu of recipes.

## License

MIT — see [LICENSE](./hw-kafka-streamly/LICENSE).

[hw-kafka-client]: https://hackage.haskell.org/package/hw-kafka-client
[librdkafka]: https://github.com/confluentinc/librdkafka
[nix]: https://nixos.org/download.html
