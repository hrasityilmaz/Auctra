# Architecture

## Overview

Auctra is a **single-node append-only engine + network server** that combines:

- log (history)
- state (current values)
- replay (stream processing)
- snapshot / restore
- binary TCP interface

It exposes both **local embedded API** and **network protocol**.

---

## Core idea

```
append-only log + current state
```

Every write is:

1. appended to WAL
2. applied to state
3. available for replay

---

## Storage model

All writes go to an append-only WAL.

Each operation is:

- PUT (key → value)
- DELETE (tombstone)

### Value handling

- small values → inline
- large values → blob storage

---

## Sharding

Keys are routed deterministically:

```
key → hash → shard
```

- no user-side shard selection
- consistent routing
- parallelizable storage
- per-shard ordering

---

## Visibility vs durability

Auctra separates write stages:

| Stage  | Meaning          |
|--------|------------------|
| Append | written to WAL   |
| Commit | visible to reads |
| Sync   | durable on disk  |

This allows:

- fast writes
- controlled durability
- predictable latency

---

## Read paths

### 1. Point lookup

Returns current value:

```
get(key)
```

---

### 2. Shard-local replay

```
readFrom(cursor)
```

- ordered per shard
- cursor-based
- efficient scanning

---

### 3. Global merged replay

```
readFromAllMerged(cursor)
```

- merges all shards
- globally ordered stream
- single unified feed

---

## Commit tokens

Each write produces a token:

```
commit_token = seqno
```

Used for:

- ordering
- replay position
- streaming cursor

---

## Network layer (Server)

Auctra exposes a binary TCP server:

```
[ FrameHeader | Payload ]
```

### Supported operations

- PING
- APPEND
- GET
- READ_FROM
- READ_FROM_ALL_MERGED
- STATS

This allows Auctra to act as:

- embedded engine
- remote data service

---

## Snapshots

Snapshots capture full state:

- fast restore
- avoids full replay
- consistent point-in-time state

---

## Checkpoints

Track engine progress:

- visibility frontier
- durability frontier

---

## Compaction

Rewrites data to remove:

- overwritten values
- tombstones
- stale records

Goals:

- reduce disk usage
- improve read performance

---

## Durability modes

- ultrafast → append only
- batch → grouped sync
- strict → fsync before return

---

## Design principles

- append-only first
- deterministic behavior
- explicit durability control
- minimal hidden work
- predictable performance

---

## Summary

Auctra provides:

- one write path
- one source of truth
- state + history together
- local + network access

Designed for:

- event sourcing
- ledgers
- audit logs
- replay pipelines
