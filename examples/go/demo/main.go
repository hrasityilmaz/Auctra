package main

import (
	"fmt"
	"log"

	"commitengine_examples/client"
)

func main() {
	c, err := client.Dial("127.0.0.1:7001")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	if err := c.Ping(); err != nil {
		log.Fatal("ping:", err)
	}
	fmt.Println("PING ok")

	if err := c.Append([]byte("user:1"), []byte("Alice"), 2); err != nil {
		log.Fatal("append:", err)
	}
	fmt.Println("APPEND ok")

	getRes, err := c.Get([]byte("user:1"))
	if err != nil {
		log.Fatal("get:", err)
	}
	fmt.Printf("GET found=%v value=%s\n", getRes.Found, string(getRes.Value))

	for shard := 0; shard < 4; shard++ {
		fmt.Println("---- shard", shard)

		res, err := c.ReadFrom(client.Cursor{
			ShardID:      uint16(shard),
			WalSegmentID: 0,
			WalOffset:    0,
		}, 16)
		if err != nil {
			log.Fatal("read_from:", err)
		}

		fmt.Printf("next_cursor=(shard=%d seg=%d off=%d)\n",
			res.NextCursor.ShardID,
			res.NextCursor.WalSegmentID,
			res.NextCursor.WalOffset,
		)

		fmt.Println("item_count:", len(res.Items))

		for i, item := range res.Items {
			fmt.Printf("item[%d] seq=%d key=%s value=%s\n",
				i,
				item.SeqNo,
				string(item.Key),
				string(item.Value),
			)
		}
	}

    shards, uptime, err := c.Stats()
    if err != nil {
        log.Fatal("stats:", err)
    }

    fmt.Println("STATS:")
    fmt.Println("  shard_count:", shards)
    fmt.Println("  uptime:", uptime)
    
    // ---------------
    mergedCursor := client.GlobalMergeCursor{
	ShardCursors: make([]client.ShardCursor, int(shards)),
    }

    mergedRes, err := c.ReadFromAllMerged(mergedCursor, 32)
    if err != nil {
        log.Fatal("read_from_all_merged:", err)
    }

    fmt.Println("MERGED READ_FROM_ALL:")
    fmt.Println("  item_count:", len(mergedRes.Items))

    for i, item := range mergedRes.Items {
        fmt.Printf("  item[%d] seq=%d key=%s value=%s\n",
            i,
            item.SeqNo,
            string(item.Key),
            string(item.Value),
        )
    }
}