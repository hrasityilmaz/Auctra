# Auctra

![Auctra](./docs/auctra_banner.png)

<h3 align="center">
⚡ Append-only log + real-time state + TCP server in a single engine
</h3>

<p align="center">
  <img src="https://img.shields.io/badge/status-developer--preview-orange" />
  <img src="https://img.shields.io/badge/language-Zig-yellow" />
  <img src="https://img.shields.io/badge/license-MIT-green" />
</p>

---

**Auctra is a log + state engine that replaces the traditional stack of log system + database + cache with a single system.**

---

## ⚡ Why not traditional architecture?

In modern architectures, you often solve a single problem with multiple disjoint systems:
- **log system** → event history  
- **database** → current state  
- **cache** → read performance  
- **pipelines** → replay & recovery  

This fragmentation leads to:

- dual writes  
- consistency drift  
- operational complexity  

**Auctra collapses this stack into a single atomic flow:**

```
Append (Log) → Apply (State) → Replay (History)
```

---

## 🚀 Key Features

- **Deterministic Replay** → reconstruct state with full accuracy  
- **Built in Zig** → zero-dependency, low-level performance  
- **Binary TCP Protocol** → efficient, low-overhead communication  
- **Snapshotting** → fast recovery without blocking  

---

## 🧠 What you can build

- Event sourcing systems  
- Financial ledgers  
- Real-time analytics pipelines  
- Audit logs  
- Streaming backfills  
- Time-travel debugging tools  

---

## 📊 Performance Benchmarks

*Benchmarks run on: Lenovo Z50 (Intel i7, 8GB RAM, Samsung 860 EVO SSD, Linux Mint, ext4, O_DIRECT WAL)*

| Operation | Throughput        | Notes                         |
|----------|------------------|-------------------------------|
| Append   | ~418k ops/sec    | Sequential disk writes        |
| Replay   | ~240k items/sec  | In-memory reconstruction      |
| Restore  | ~230 ms          | 100k records (snapshot load)  |

---

## 🛠 Technical Overview

### Durability Modes

- **Ultrafast** → max throughput, async durability  
- **Batch** → balanced fsync strategy  
- **Strict** → fsync per write (max safety)  

---

### Feature Matrix

| Feature                        | Core (OSS) | Pro |
|--------------------------------|:----------:|:---:|
| Single Write Path              |     ✅     | ✅  |
| Deterministic Replay           |     ✅     | ✅  |
| Snapshotting                   |     ✅     | ✅  |
| Replication (Leader/Follower)  |     ❌     | ✅  |
| Distributed Consensus (Raft)   |     ❌     | ✅  |

---

## 🧪 Quick Start

### 1. Build
Requires Zig 0.15+

```bash
zig build -Doptimize=ReleaseFast
```

### 2. Run server

```bash
./auctra-core server
```

### 3. Run client

```bash
python3 examples/python/client_demo.py
```

---

## 🚧 Advanced Features

Replication, clustering, and distributed capabilities are part of **Auctra Pro**.

---

## License

MIT
