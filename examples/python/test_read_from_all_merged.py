from commit_engine import CommitEngine, Cursor


def make_zero_cursors(shard_count: int) -> list[Cursor]:
    return [Cursor(shard_id=i, wal_segment_id=0, wal_offset=0) for i in range(shard_count)]


def test_read_from_all_merged_smoke() -> None:
    with CommitEngine(shard_count=4, inline_max=128) as engine:
        engine.put(0, b"u:0", b"a")
        engine.put(1, b"u:1", b"b")
        engine.put(0, b"u:0", b"c")
        engine.delete(1, b"u:1")

        start = make_zero_cursors(4)
        items, next_cursors = engine.read_from_all_merged(start, limit=10)

        assert len(items) >= 4
        assert len(next_cursors) == 4

        seqs = [x.seqno for x in items]
        assert seqs == sorted(seqs), f"not globally sorted: {seqs}"


if __name__ == "__main__":
    test_read_from_all_merged_smoke()
    print("ok")
