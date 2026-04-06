from auctra_core import CommitEngine, Cursor


def cur(c: Cursor) -> str:
    return f"({c.shard_id},{c.wal_segment_id},{c.wal_offset})"


def main() -> None:
    with CommitEngine(shard_count=4, inline_max=128) as engine:
        shard_id = 0
        key = b"user:42"

        print("sizeof Cursor =", __import__("ctypes").sizeof(Cursor))

        print("== create ==")
        create_window = engine.put(shard_id, key, b"email=alice@example.com|name=Alice")
        print(create_window.first_seqno, create_window.last_seqno, create_window.record_count)
        print("create visible =", cur(create_window.visible))
        print("create durable =", cur(create_window.durable), "has=", create_window.has_durable)

        print("== update ==")
        update_window = engine.put(shard_id, key, b"email=alice@newdomain.com|name=Alice")
        print(update_window.first_seqno, update_window.last_seqno, update_window.record_count)
        print("update visible =", cur(update_window.visible))
        print("update durable =", cur(update_window.durable), "has=", update_window.has_durable)

        print("== delete ==")
        delete_window = engine.delete(shard_id, key)
        print(delete_window.first_seqno, delete_window.last_seqno, delete_window.record_count)
        print("delete visible =", cur(delete_window.visible))
        print("delete durable =", cur(delete_window.durable), "has=", delete_window.has_durable)

        print("== replay visible, paged debug ==")
        cursor = Cursor(shard_id=shard_id, wal_segment_id=0, wal_offset=0)

        for page_no in range(6):
            items, next_cursor = engine.read_from(cursor, limit=2)
            print(
                f"PAGE {page_no}: start={cur(cursor)} next={cur(next_cursor)} count={len(items)}"
            )

            if not items:
                print("END")
                break

            for idx, item in enumerate(items):
                print(
                    f"  item[{idx}] "
                    f"seq={item.seqno} "
                    f"cursor={cur(item.cursor)} "
                    f"key={item.key!r} "
                    f"value={item.value!r} "
                    f"record={item.record_type} "
                    f"durability={item.durability}"
                )

            if (
                next_cursor.shard_id == cursor.shard_id
                and next_cursor.wal_segment_id == cursor.wal_segment_id
                and next_cursor.wal_offset == cursor.wal_offset
            ):
                print("NON-ADVANCING CURSOR DETECTED")
                break

            cursor = next_cursor


if __name__ == "__main__":
    main()