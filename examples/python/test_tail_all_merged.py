from auctra_core_with_tail import CommitEngine, Cursor


def zero_cursors(n: int):
    return [Cursor(shard_id=i, wal_segment_id=0, wal_offset=0) for i in range(n)]


def test_tail_all_merged_smoke() -> None:
    with CommitEngine(shard_count=4, inline_max=128) as engine:
        engine.put(0, b"tail:a", b"v1")
        engine.put(1, b"tail:b", b"v2")
        engine.delete(1, b"tail:b")

        start = zero_cursors(4)

        seen = []
        for item in engine.tail_all_merged(
            start,
            page_size=3,
            poll_interval=0.01,
            idle_round_limit=5,
        ):
            seen.append(item)
            if len(seen) >= 3:
                break

        assert len(seen) >= 2, f"expected >= 2 items, got {len(seen)}"

        seqs = [x.seqno for x in seen]
        assert seqs == sorted(seqs), f"seqs not sorted: {seqs}"

        assert any(x.key == b"tail:a" for x in seen), "missing tail:a"
        assert any(x.key == b"tail:b" for x in seen), "missing tail:b"