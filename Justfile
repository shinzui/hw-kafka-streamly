# Justfile for hw-kafka-streamly

# Default recipe to display help
default:
    @just --list


# --- Services ---

# Start Redpanda via process-compose
[group("services")]
process-up:
    process-compose --tui=false --unix-socket .dev/process-compose.sock up

# Stop Redpanda
[group("services")]
process-down:
    process-compose --unix-socket .dev/process-compose.sock down || true


# --- Kafka (rpk) ---

# Create the jitsurei-topic
[group("kafka")]
create-topic:
    rpk topic create jitsurei-topic -p 1

# Create the output topic
[group("kafka")]
create-output-topic:
    rpk topic create jitsurei-streamly-output -p 1

# Delete the jitsurei-topic
[group("kafka")]
delete-topic:
    rpk topic delete jitsurei-topic

# List all topics
[group("kafka")]
list-topics:
    rpk topic list


# --- Build ---

# Build all packages
[group("build")]
build:
    cabal build all

# Clean build artifacts
[group("build")]
clean:
    cabal clean

# Format code
[group("build")]
fmt:
    nix fmt
