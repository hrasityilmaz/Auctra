# Architecture

## Overview

Auctra-Core is a single-node append-only engine that combines:

* log (history)
* state (current values)
* replay (stream processing)
* snapshot / restore

---

## Storage model

All writes go to an append-only WAL.

Each operation is:

* PUT (key → value)
* DELETE (tombstone)

### Value handling

* small values → inline
* large values → blob storage

---

## Sharding

Keys are routed deterministically:

```text
key → hash → shard
```

* no user-side shard selection
* consistent routing
* parallelizable storage

---

## Visibility vs durability

Auctra separates:

| Stage  | Meaning          |
| ------ | ---------------- |
| Append | written to WAL   |
| Commit | visible to reads |
| Sync   | durable on disk  |

This allows:

* fast writes
* controlled durability
* predictable latency

---

## Read paths

### Point lookup

Returns current value:

```text
get(key)
```

---

### Replay

Reads historical events:

```text
readFrom(cursor)
```

---

### Merged replay

Combines shard streams into a single ordered feed.

---

## Snapshots

Snapshots capture full state:

* fast restore
* avoids full replay
* consistent state point

---

## Checkpoints

Track engine progress:

* visibility frontier
* durability frontier

---

## Compaction

Rewrites data to remove:

* overwritten values
* tombstones
* stale records

Goals:

* reduce disk
* improve read speed

---

## Durability modes

* ultrafast → append only
* batch → grouped sync
* strict → sync before return

---

## Core concept

```text
log + state in one engine
```

Instead of:

* Kafka (log)
* * DB (state)

Auctra provides both.

---

## Design principles

* append-only first
* deterministic behavior
* explicit durability control
* minimal hidden work
* predictable performance

---

## Summary

Auctra is designed for:

* event sourcing
* ledgers
* audit logs
* real-time state + history

It provides:

* one write path
* one source of truth
* both state and history
