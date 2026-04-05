![Auctra](./docs/auctra_banner.png)

<h3 align="center">
⚡ Append-only log + real-time state in a single engine
</h3>

<p align="center">
  <img src="https://img.shields.io/badge/status-developer--preview-orange" />
  <img src="https://img.shields.io/badge/language-Zig-yellow" />
  <img src="https://img.shields.io/badge/license-MIT-green" />
</p>

---

# Auctra

**Auctra is an append-only log + state engine.**

It replaces the common stack of:

- log systems (Kafka, etc.)
- databases
- caches
- replay pipelines

with a **single deterministic engine**.

> One write path. One source of truth. Full history + real-time state.

---

## Why Auctra exists

Modern systems need:

- full event history
- fast current state

Typical architecture:

- Kafka → history  
- DB → current state  
- cache → performance  
- pipelines → replay  

Problems:

- complex infrastructure  
- duplicated data paths  
- consistency issues  
- hard debugging  

---

## The Auctra model

Auctra simplifies everything:

**append-only log + current state**

Every write:

1. appended to WAL  
2. applied to state  
3. available for replay  

No dual systems. No sync layers. No divergence.

---

## What makes Auctra different

Auctra is not:

- just a database  
- just a log  
- just a cache  

It is all three.

### Key properties

- single write path  
- deterministic replay  
- instant state access  
- full history retention  
- crash-consistent durability modes  

---

## What you can build

- event sourcing systems  
- financial ledgers  
- audit trails  
- streaming pipelines  
- real-time materialized views  
- deterministic backfills  
- time-travel debugging  

---

## Core concepts

### Single write path

write → WAL → state → replay  

Guarantees:

- consistency  
- traceability  
- determinism  

---

### Write lifecycle

1. append → written to WAL  
2. commit → visible  
3. sync → durable  

---

### Durability modes

| Mode      | Visibility   | Durability            |
|----------|-------------|-----------------------|
| ultrafast | after commit | async                |
| batch     | after commit | grouped fsync        |
| strict    | after commit | fsync before return  |

---

## Performance

### Append throughput

- ultrafast: ~418k ops/sec  
- batch: ~440k ops/sec  
- strict: ~390k ops/sec  

### Replay

- merged replay: ~240k items/sec  

### Snapshot

- save (100k): ~1.7 s  
- restore (100k): ~230 ms  

---

## 🧪 Auctra Server (Preview)

Auctra includes a **binary TCP server**.

### Run server

```
./auctra-core server
```

Server runs on:

```
127.0.0.1:7001
```

---

## Protocol

Binary framed protocol:

```
[ FrameHeader | Payload ]
```

### Operations

- PING → health check  
- APPEND → write  
- GET → current state  
- READ_FROM → shard replay  
- READ_FROM_ALL_MERGED → global replay  
- STATS → metrics  

---

## Example (Go)

```
./auctra-core server
cd examples/go
go run ./demo
```

### Output

```
PING ok

APPEND ok
commit_token: 10

GET found=true value=Tengri
```

---

## Key idea

Auctra exposes:

- current state → GET  
- full history → READ  

Nothing is lost. Everything is replayable.

---

## Commit tokens

Each append returns:

```
commit_token = seqno
```

Used for:

- ordering  
- replay position  
- streaming cursor  

---

## Stats example

```
shard_count: 4
uptime: 18
```

---

## Architecture

See:

```
docs/architecture.md
```

---

## Status

**Developer Preview**

---

## License

MIT
