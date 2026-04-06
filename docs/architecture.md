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

```
client → WAL → state → replay
```

1. appended to WAL  
2. applied to state  
3. available for replay  

---

## Data flow diagram

```
           ┌──────────────┐
           │   Client     │
           └──────┬───────┘
                  │ APPEND
                  ▼
           ┌──────────────┐
           │     WAL      │
           │ (append-only)│
           └──────┬───────┘
                  │
        ┌─────────┴─────────┐
        ▼                   ▼
┌──────────────┐     ┌──────────────┐
│    State     │     │   Replay     │
│ (KV store)   │     │ (streaming)  │
└──────────────┘     └──────────────┘
```

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

## Commit lifecycle

```
append → commit → durable
```

| Stage   | Meaning                |
|---------|------------------------|
| Append  | written to WAL         |
| Commit  | visible to reads       |
| Durable | fsync completed        |

---

## Commit window (concept)

Each write produces a structured result:

```
CommitWindow {
  seqno
  shard_id
  wal_position
  visible_position
  durable_position
}
```

This allows:

- precise tracking
- replay boundaries
- deterministic recovery

---

## Read paths

### 1. Point lookup

```
get(key)
```

Returns current value.

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

## Cursor model

```
Cursor {
  shard_id
  wal_segment_id
  wal_offset
}
```

Used for:

- replay position
- streaming continuation
- incremental reads

---

## Network layer (Server)

Binary protocol:

```
[ FrameHeader | Payload ]
```

### Protocol v2 (current)

Append response:

```
seqno
shard_id
wal_segment_id
wal_offset
visible_segment_id
visible_offset
durable_segment_id
durable_offset
record_count
```

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

| Mode      | Behavior               |
|-----------|------------------------|
| ultrafast | async write            |
| batch     | grouped fsync          |
| strict    | fsync before return    |

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
