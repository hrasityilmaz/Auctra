package main

import (
	"fmt"
	"log"

	"auctra_examples/auctra"
)

func main() {
	e, err := auctra.OpenDefault()
	if err != nil {
		log.Fatal("open:", err)
	}
	defer e.Close()

	appendRes, err := e.Put([]byte("user:1"), []byte("Tengri"), true)
	if err != nil {
		log.Fatal("put:", err)
	}

	fmt.Println("PUT ok")
	fmt.Println("shard_id:", appendRes.ShardID)
	fmt.Println("start:", appendRes.Start.WALSegmentID, appendRes.Start.WALOffset)
	fmt.Println("end:", appendRes.End.WALSegmentID, appendRes.End.WALOffset)
	fmt.Println("visible:", appendRes.Visible.WALSegmentID, appendRes.Visible.WALOffset)
	if appendRes.HasDurable {
		fmt.Println("durable:", appendRes.Durable.WALSegmentID, appendRes.Durable.WALOffset)
	}
	fmt.Println("record_count:", appendRes.RecordCount)
	fmt.Println("first_seqno:", appendRes.FirstSeqno)
	fmt.Println("last_seqno:", appendRes.LastSeqno)

	value, err := e.Get([]byte("user:1"))
	if err != nil {
		log.Fatal("get:", err)
	}
	fmt.Printf("GET value=%s\n", string(value))

	for shard := 0; shard < 4; shard++ {
		fmt.Println("---- shard", shard)

		items, next, err := e.ReadFrom(auctra.Cursor{
			ShardID:      uint16(shard),
			WALSegmentID: 0,
			WALOffset:    0,
		}, 16)
		if err != nil {
			log.Fatal("read_from:", err)
		}

		fmt.Printf("next_cursor=(shard=%d seg=%d off=%d)\n",
			next.ShardID,
			next.WALSegmentID,
			next.WALOffset,
		)

		fmt.Println("item_count:", len(items))

		for i, item := range items {
			fmt.Printf("item[%d] seq=%d key=%s value=%s tombstone=%v\n",
				i,
				item.Seqno,
				string(item.Key),
				string(item.Value),
				item.Tombstone,
			)
		}
	}

	start := []auctra.Cursor{
		{ShardID: 0, WALSegmentID: 0, WALOffset: 0},
		{ShardID: 1, WALSegmentID: 0, WALOffset: 0},
		{ShardID: 2, WALSegmentID: 0, WALOffset: 0},
		{ShardID: 3, WALSegmentID: 0, WALOffset: 0},
	}

	mergedItems, nextCursors, err := e.ReadFromAllMerged(start, 32)
	if err != nil {
		log.Fatal("read_from_all_merged:", err)
	}

	fmt.Println("MERGED READ_FROM_ALL:")
	fmt.Println("  item_count:", len(mergedItems))

	for i, item := range mergedItems {
		fmt.Printf("  item[%d] seq=%d key=%s value=%s\n",
			i,
			item.Seqno,
			string(item.Key),
			string(item.Value),
		)
	}

	fmt.Println("  next_cursors:")
	for i, c := range nextCursors {
		fmt.Printf("    [%d] shard=%d seg=%d off=%d\n",
			i, c.ShardID, c.WALSegmentID, c.WALOffset)
	}
}