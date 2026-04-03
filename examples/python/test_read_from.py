from commit_engine import CommitEngine, Cursor


def ctuple(c: Cursor) -> tuple[int, int, int]:
    return (c.shard_id, c.wal_segment_id, c.wal_offset)


def test_read_from_progresses() -> None:
    with CommitEngine(shard_count=4, inline_max=128) as engine:
        shard_id = 0
        key = b"user:test_progress"

        w1 = engine.put(shard_id, key, b"v1")
        engine.put(shard_id, key, b"v2")
        engine.delete(shard_id, key)

        start = w1.start
        items, next_cursor = engine.read_from(start, limit=2)

        assert len(items) > 0, "expected at least one replay item"
        assert ctuple(next_cursor) != ctuple(start), "next cursor must advance"

        for item in items:
            assert item.cursor.shard_id == shard_id, f"bad shard id: {ctuple(item.cursor)}"
            assert item.cursor.wal_offset > 0, f"bad wal offset: {ctuple(item.cursor)}"


def test_read_from_pages_terminate() -> None:
    with CommitEngine(shard_count=4, inline_max=128) as engine:
        shard_id = 0
        key = b"user:test_pages"

        w1 = engine.put(shard_id, key, b"v1")
        engine.put(shard_id, key, b"v2")
        engine.delete(shard_id, key)

        cursor = w1.start

        seen = 0
        for _ in range(10):
            items, next_cursor = engine.read_from(cursor, limit=1)
            if not items:
                break

            seen += len(items)

            assert ctuple(next_cursor) != ctuple(cursor), (
                f"non-advancing cursor: start={ctuple(cursor)} next={ctuple(next_cursor)}"
            )

            cursor = next_cursor
        else:
            raise AssertionError("read_from did not terminate within 10 pages")

        assert seen == 3, f"expected exactly 3 replay items, saw {seen}"


def test_replay_contains_tombstone() -> None:
    with CommitEngine(shard_count=4, inline_max=128) as engine:
        shard_id = 0
        key = b"user:test_tombstone"

        w1 = engine.put(shard_id, key, b"v1")
        engine.put(shard_id, key, b"v2")
        engine.delete(shard_id, key)

        items = list(engine.replay(w1.start, page_size=2))

        assert len(items) == 3, f"expected exactly 3 replay items, saw {len(items)}"
        assert items[-1].record_type == "tombstone", f"last record type was {items[-1].record_type}"
        assert items[-1].value is None, f"tombstone value should be None, got {items[-1].value!r}"


if __name__ == "__main__":
    test_read_from_progresses()
    test_read_from_pages_terminate()
    test_replay_contains_tombstone()
    print("ok")