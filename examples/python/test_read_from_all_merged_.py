from commit_engine_tail_clean import CommitEngine, Cursor


def zero_cursors(n: int):
    return [Cursor(shard_id=i, wal_segment_id=0, wal_offset=0) for i in range(n)]


def test_read_from_all_merged_eventually_contains_tombstone() -> None:
    with CommitEngine(shard_count=4, inline_max=128) as engine:
        engine.put(0, b"tail:a", b"v1")
        engine.put(1, b"tail:b", b"v2")
        engine.delete(1, b"tail:b")

        cursors = zero_cursors(4)
        seen = []

        for _ in range(10):
            items, cursors = engine.read_from_all_merged(cursors, limit=2)
            if not items:
                break
            seen.extend(items)

        assert any(x.record_type == "tombstone" for x in seen), "missing tombstone"