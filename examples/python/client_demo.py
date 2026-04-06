from client import AuctraClient, Cursor, GlobalMergeCursor, ShardCursor


def main() -> None:
    with AuctraClient("127.0.0.1", 7001) as c:
        c.ping()
        print("PING ok")

        append_res = c.append(b"user:1", b"Tengri", 2)
        print("APPEND ok")
        print("seqno:", append_res.seqno)
        print("shard_id:", append_res.shard_id)
        print("wal:", append_res.wal_segment_id, append_res.wal_offset)
        print("visible:", append_res.visible_segment_id, append_res.visible_offset)
        print("durable:", append_res.durable_segment_id, append_res.durable_offset)
        print("record_count:", append_res.record_count)

        get_res = c.get(b"user:1")
        print(f"GET found={get_res.found} value={get_res.value.decode()}")

        for shard in range(4):
            print("---- shard", shard)
            res = c.read_from(
                Cursor(
                    shard_id=shard,
                    wal_segment_id=0,
                    wal_offset=0,
                ),
                16,
            )

            print(
                f"next_cursor=(shard={res.next_cursor.shard_id} "
                f"seg={res.next_cursor.wal_segment_id} "
                f"off={res.next_cursor.wal_offset})"
            )
            print("item_count:", len(res.items))

            for i, item in enumerate(res.items):
                print(
                    f"item[{i}] seq={item.seqno} "
                    f"key={item.key.decode()} "
                    f"value={item.value.decode()}"
                )

        shard_count, uptime = c.stats()
        print("STATS:")
        print("  shard_count:", shard_count)
        print("  uptime:", uptime)

        merged_cursor = GlobalMergeCursor(
            shard_cursors=[ShardCursor(wal_segment_id=0, wal_offset=0) for _ in range(shard_count)]
        )

        merged_res = c.read_from_all_merged(merged_cursor, 32)
        print("MERGED READ_FROM_ALL:")
        print("  item_count:", len(merged_res.items))

        for i, item in enumerate(merged_res.items):
            print(
                f"  item[{i}] seq={item.seqno} "
                f"key={item.key.decode()} "
                f"value={item.value.decode()}"
            )


if __name__ == "__main__":
    main()