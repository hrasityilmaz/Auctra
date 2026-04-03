package main

import (
    "fmt"
    "log"

    ce "commitengine_examples/commitengine"
)

func main() {
    engine, err := ce.Open(4, 128)
    if err != nil {
        log.Fatal(err)
    }
    defer engine.Close()

    key := []byte("user:go-demo")

    fmt.Println("== create ==")
    w1, _ := engine.Put(key, []byte("v1"), true)

    fmt.Println("== update ==")
    engine.Put(key, []byte("v2"), true)

    fmt.Println("== delete ==")
    engine.Delete(key, true)

    fmt.Println("== replay ==")

    cursor := w1.Start

    for {
        items, next, err := engine.ReadFrom(cursor, 2)
        if err != nil {
            log.Fatal(err)
        }

        if len(items) == 0 {
            fmt.Println("END")
            break
        }

        fmt.Printf("PAGE cursor=(%d,%d,%d)\n",
            cursor.ShardID,
            cursor.WALSegmentID,
            cursor.WALOffset,
        )

        for _, item := range items {
            fmt.Printf(
                "  seq=%d cursor=(%d,%d,%d) key=%q tombstone=%v value=%q\n",
                item.Seqno,
                item.Cursor.ShardID,
                item.Cursor.WALSegmentID,
                item.Cursor.WALOffset,
                item.Key,
                item.Tombstone,
                item.Value,
            )
        }

        cursor = next
    }
}