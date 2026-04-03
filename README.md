
![Auctra](./docs/auctra_banner.png)

<h3 align="center">
⚡ Append-only log + real-time state in a single engine
</h3>

<p align="center">
  <img src="https://img.shields.io/badge/status-experimental-orange" />
  <img src="https://img.shields.io/badge/language-Zig-yellow" />
  <img src="https://img.shields.io/badge/license-MIT-green" />
</p>

**Auctra is an append-only log + state engine.**

Auctra sits between log systems and databases.

It combines:

* a write-ahead log (WAL)
* a queryable key-value state
* snapshots and checkpoints
* deterministic replay

---

## Why Auctra?

Most systems force you to choose:

* **log systems** (great for history)
* **state stores / databases** (great for current state)

Auctra gives you both in a single engine.

---

## Core idea

```text
append-only log + current state
```

Every write is:

* appended to a log
* reflected in current state
* available for replay

---

## What you can build

* event sourcing systems
* financial ledgers
* audit logs
* real-time state + history pipelines
* embedded stream processors

---

## Guarantees

Auctra distinguishes three write states:

| State    | Meaning                                        |
| -------- | ---------------------------------------------- |
| Appended | The write was appended to the WAL              |
| Visible  | The write is visible to reads                  |
| Durable  | The write is expected to survive crash/restart |

---

### Write lifecycle

1. append → data written to WAL
2. commit → data becomes visible
3. sync → data becomes durable

---

### High-level API

* `putWithDurability(...)` → append only
* `putVisible(...)` → append + commit
* `commit()` → make writes visible
* `sync()` → make writes durable

---

### Durability modes

| Mode      | Visibility   | Durability            |
| --------- | ------------ | --------------------- |
| ultrafast | after commit | later                 |
| batch     | after commit | batched sync          |
| strict    | after commit | ensured before return |

---

## Benchmark

All benchmarks are included in `./zig-out/bin/bench`.

Single-node benchmark on a local machine:

### Append throughput

```text
ultrafast: ~418k ops/sec
batch:     ~440k ops/sec
strict:    ~390k ops/sec
```

### Replay

```text
merged replay: ~244k items/sec
```

### Snapshot

```text
save (100k entries):    ~1.7 s
restore (100k entries): ~230 ms
```

---

### Notes

* These are single-node measurements
* No networking or replication involved
* Numbers depend on hardware and configuration

The goal is to provide:

* high write throughput
* fast state recovery
* efficient replay

Designed for high-throughput append workloads and fast recovery.

---

## Example

```bash
./auctra-core put user:1 Tengri
./auctra-core get user:1
```

---

## Why this exists

Modern systems often need both:

* full event history
* fast current state

This usually leads to:

* log systems + databases
* multiple layers for state, caching, and replay

Auctra simplifies this:

* one engine
* one write path
* one source of truth

Auctra removes the need to combine multiple systems to achieve both history and state.

---

## Architecture

See: `docs/architecture.md`

---

## Status

V1 is complete:

* append-only WAL ✔
* state + replay ✔
* snapshot / restore ✔
* compaction ✔
* C / Python / Go bindings ✔

---

## License

MIT
