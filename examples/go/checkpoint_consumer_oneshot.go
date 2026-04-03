package main

import (
	"fmt"

	ce "commitengine_examples/commitengine"
)

const checkpointPath = "/tmp/orders-consumer.ckpt"

func loadOrCapture(engine *ce.Engine, path string) (*ce.Checkpoint, error) {
	cp, err := ce.LoadCheckpoint(path)
	if err == nil {
		return cp, nil
	}
	return engine.CaptureCheckpoint(true)
}

func main() {
	engine, err := ce.Open(4, 16)
	if err != nil {
		panic(err)
	}
	defer engine.Close()

	cp, err := loadOrCapture(engine, checkpointPath)
	if err != nil {
		panic(err)
	}

	items, nextCP, err := engine.ReadFromCheckpointEx(cp, 128)
	if err != nil {
		panic(err)
	}

	fmt.Printf("items=%d\n", len(items))
	for _, item := range items {
		fmt.Printf("seq=%d key=%s tombstone=%v\n", item.Seqno, string(item.Key), item.Tombstone)
	}

	if nextCP != nil {
		fmt.Printf("next checkpoint cursors=%+v\n", nextCP.Cursors)
	}
}