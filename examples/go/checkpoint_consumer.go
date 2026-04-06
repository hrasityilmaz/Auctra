package main

import (
	"fmt"
	"os"
	"time"

	ce "auctra_examples/auctra"
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

	for {
		items, nextCP, err := engine.ReadFromCheckpointEx(cp, 128)
		if err != nil {
			panic(err)
		}

		if len(items) == 0 {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		for _, item := range items {
			fmt.Printf("seq=%d key=%s tombstone=%v\n", item.Seqno, string(item.Key), item.Tombstone)
		}

		tmp := checkpointPath + ".tmp"
		if err := ce.SaveCheckpointData(tmp, nextCP); err != nil {
			panic(err)
		}
		if err := os.Rename(tmp, checkpointPath); err != nil {
			panic(err)
		}

		cp = nextCP
	}
}
