package commitengine

import "testing"

func TestReadFromProgresses(t *testing.T) {
    engine, err := Open(4, 128)
    if err != nil {
        t.Fatal(err)
    }
    defer engine.Close()

    key := []byte("user:test-go")

    w1, _ := engine.Put(key, []byte("v1"), true)
    engine.Put(key, []byte("v2"), true)
    engine.Delete(key, true)

    start := w1.Start

    items, next, err := engine.ReadFrom(start, 2)
    if err != nil {
        t.Fatal(err)
    }

    if len(items) == 0 {
        t.Fatal("expected items")
    }

    if next == start {
        t.Fatal("cursor did not advance")
    }
}

func TestReplayTerminates(t *testing.T) {
    engine, err := Open(4, 128)
    if err != nil {
        t.Fatal(err)
    }
    defer engine.Close()

    key := []byte("user:test-go-2")

    w1, _ := engine.Put(key, []byte("v1"), true)
    engine.Put(key, []byte("v2"), true)
    engine.Delete(key, true)

    cursor := w1.Start

    count := 0

    for i := 0; i < 10; i++ {
        items, next, err := engine.ReadFrom(cursor, 1)
        if err != nil {
            t.Fatal(err)
        }

        if len(items) == 0 {
            break
        }

        if next == cursor {
            t.Fatalf("non advancing cursor: %+v", cursor)
        }

        count += len(items)
        cursor = next
    }

    if count != 3 {
        t.Fatalf("expected 3 items, got %d", count)
    }
}