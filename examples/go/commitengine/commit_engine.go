package commitengine

/*
#cgo CFLAGS: -I${SRCDIR}/../../../include
#cgo LDFLAGS: -L${SRCDIR}/../../../zig-out/lib -lauctra_core
#include <commit_engine.h>
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"time"
	"unsafe"
)

const (
	readVisible = 0
	readDurable = 1

	valueTombstone = 2
)

type Cursor struct {
	ShardID      uint16
	WALSegmentID uint32
	WALOffset    uint64
}

type CommitWindow struct {
	ShardID     uint16
	Start       Cursor
	End         Cursor
	Visible     Cursor
	Durable     Cursor
	HasDurable  bool
	RecordCount uint64
	FirstSeqno  uint64
	LastSeqno   uint64
}

type ReplayItem struct {
	Seqno      uint64
	Cursor     Cursor
	Key        []byte
	Value      []byte
	Tombstone  bool
	RecordType uint8
	ValueKind  uint8
	Durability uint8
}

type MergedReplayItem struct {
	Seqno        uint64
	SourceCursor Cursor
	Key          []byte
	Value        []byte
	Tombstone    bool
	RecordType   uint8
	ValueKind    uint8
	Durability   uint8
}

type Checkpoint struct {
	Version         uint32
	ShardCount      uint16
	Kind            uint8
	CreatedAtUnixNs uint64
	HighSeqno       uint64
	Cursors         []Cursor
}

type Engine struct{ handle *C.CeEngine }

func Open(shardCount uint16, inlineMax uint32) (*Engine, error) {
	h := C.ce_engine_open(C.uint16_t(shardCount), C.uint32_t(inlineMax))
	if h == nil {
		return nil, errors.New("failed to open commit engine")
	}
	return &Engine{handle: h}, nil
}

func OpenDefault() (*Engine, error) {
	h := C.ce_engine_open_default()
	if h == nil {
		return nil, errors.New("failed to open commit engine")
	}
	return &Engine{handle: h}, nil
}

func (e *Engine) Close() {
	if e == nil || e.handle == nil {
		return
	}
	C.ce_engine_close(e.handle)
	e.handle = nil
}

func statusErr(status C.int) error {
	if status == 0 {
		return nil
	}
	return fmt.Errorf("commit engine error: %s (%d)", C.GoString(C.ce_status_string(status)), int(status))
}

func toCursor(c C.CeCursor) Cursor {
	return Cursor{ShardID: uint16(c.shard_id), WALSegmentID: uint32(c.wal_segment_id), WALOffset: uint64(c.wal_offset)}
}

func toCCursor(c Cursor) C.CeCursor {
	return C.CeCursor{shard_id: C.uint16_t(c.ShardID), wal_segment_id: C.uint32_t(c.WALSegmentID), wal_offset: C.uint64_t(c.WALOffset)}
}

func toWindow(w C.CeCommitWindow) CommitWindow {
	return CommitWindow{
		ShardID:     uint16(w.shard_id),
		Start:       toCursor(w.start),
		End:         toCursor(w.end),
		Visible:     toCursor(w.visible),
		Durable:     toCursor(w.durable),
		HasDurable:  w.has_durable != 0,
		RecordCount: uint64(w.record_count),
		FirstSeqno:  uint64(w.first_seqno),
		LastSeqno:   uint64(w.last_seqno),
	}
}

func asPtr(b []byte) *C.uint8_t {
	if len(b) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0]))
}

func (e *Engine) Put(key, value []byte, awaitDurable bool) (CommitWindow, error) {
	var out C.CeCommitWindow
	var durable C.uint8_t
	if awaitDurable {
		durable = 1
	}
	status := C.ce_put(e.handle, asPtr(key), C.size_t(len(key)), asPtr(value), C.size_t(len(value)), durable, &out)
	return toWindow(out), statusErr(status)
}

func (e *Engine) Delete(key []byte, awaitDurable bool) (CommitWindow, error) {
	var out C.CeCommitWindow
	var durable C.uint8_t
	if awaitDurable {
		durable = 1
	}
	status := C.ce_delete(e.handle, asPtr(key), C.size_t(len(key)), durable, &out)
	return toWindow(out), statusErr(status)
}

func (e *Engine) Get(key []byte) ([]byte, error) {
	var out C.CeBuffer
	status := C.ce_get(e.handle, asPtr(key), C.size_t(len(key)), &out)
	if int(status) == 4 {
		return nil, nil
	}
	if err := statusErr(status); err != nil {
		return nil, err
	}
	defer C.ce_buffer_free(&out)
	if out.ptr == nil {
		return []byte{}, nil
	}
	return C.GoBytes(unsafe.Pointer(out.ptr), C.int(out.len)), nil
}

func (e *Engine) GetAt(cursor Cursor, key []byte) ([]byte, error) {
	var out C.CeBuffer
	status := C.ce_get_at(e.handle, toCCursor(cursor), asPtr(key), C.size_t(len(key)), &out)
	if int(status) == 4 {
		return nil, nil
	}
	if err := statusErr(status); err != nil {
		return nil, err
	}
	defer C.ce_buffer_free(&out)
	if out.ptr == nil {
		return []byte{}, nil
	}
	return C.GoBytes(unsafe.Pointer(out.ptr), C.int(out.len)), nil
}

func (e *Engine) ReadFrom(start Cursor, limit int) ([]ReplayItem, Cursor, error) {
	return e.readFromConsistent(start, limit, readVisible)
}

func (e *Engine) ReadFromDurable(start Cursor, limit int) ([]ReplayItem, Cursor, error) {
	return e.readFromConsistent(start, limit, readDurable)
}

func (e *Engine) readFromConsistent(start Cursor, limit int, consistency int) ([]ReplayItem, Cursor, error) {
	var out C.CeReadResult
	status := C.ce_read_from_consistent(e.handle, toCCursor(start), C.size_t(limit), C.CeReadConsistency(consistency), &out)
	if err := statusErr(status); err != nil {
		return nil, Cursor{}, err
	}
	defer C.ce_read_result_free(&out)

	next := Cursor{ShardID: uint16(out.next_shard_id), WALSegmentID: uint32(out.next_wal_segment_id), WALOffset: uint64(out.next_wal_offset)}
	if out.items == nil || out.item_count == 0 {
		return nil, next, nil
	}

	rawItems := unsafe.Slice((*C.CeStreamItem)(unsafe.Pointer(out.items)), int(out.item_count))
	items := make([]ReplayItem, 0, len(rawItems))
	for _, raw := range rawItems {
		key := []byte{}
		if raw.key_ptr != nil && raw.key_len > 0 {
			key = C.GoBytes(unsafe.Pointer(raw.key_ptr), C.int(raw.key_len))
		}
		tombstone := uint8(raw.value_kind) == valueTombstone
		var value []byte
		if !tombstone {
			if raw.value_ptr != nil && raw.value_len > 0 {
				value = C.GoBytes(unsafe.Pointer(raw.value_ptr), C.int(raw.value_len))
			} else {
				value = []byte{}
			}
		}
		items = append(items, ReplayItem{
			Seqno:      uint64(raw.seqno),
			Cursor:     Cursor{ShardID: uint16(raw.cursor_shard_id), WALSegmentID: uint32(raw.cursor_wal_segment_id), WALOffset: uint64(raw.cursor_wal_offset)},
			Key:        key,
			Value:      value,
			Tombstone:  tombstone,
			RecordType: uint8(raw.record_type),
			ValueKind:  uint8(raw.value_kind),
			Durability: uint8(raw.durability),
		})
	}
	return items, next, nil
}

func (e *Engine) ReadFromAllMerged(start []Cursor, limit int) ([]MergedReplayItem, []Cursor, error) {
	return e.readFromAllMergedConsistent(start, limit, readVisible)
}

func (e *Engine) ReadFromAllMergedDurable(start []Cursor, limit int) ([]MergedReplayItem, []Cursor, error) {
	return e.readFromAllMergedConsistent(start, limit, readDurable)
}

func (e *Engine) readFromAllMergedConsistent(start []Cursor, limit int, consistency int) ([]MergedReplayItem, []Cursor, error) {
	if len(start) == 0 {
		return nil, nil, errors.New("start cursors must not be empty")
	}
	cCursors := make([]C.CeCursor, len(start))
	for i, c := range start {
		cCursors[i] = toCCursor(c)
	}

	var out C.CeMergedReadResult
	status := C.ce_read_from_all_merged_consistent(e.handle, &cCursors[0], C.size_t(len(cCursors)), C.size_t(limit), C.CeReadConsistency(consistency), &out)
	if err := statusErr(status); err != nil {
		return nil, nil, err
	}
	defer C.ce_merged_read_result_free(&out)

	next := make([]Cursor, 0, int(out.next_cursor_count))
	if out.next_cursors != nil && out.next_cursor_count > 0 {
		for _, c := range unsafe.Slice((*C.CeCursor)(unsafe.Pointer(out.next_cursors)), int(out.next_cursor_count)) {
			next = append(next, toCursor(c))
		}
	}

	items := make([]MergedReplayItem, 0, int(out.item_count))
	if out.items != nil && out.item_count > 0 {
		for _, raw := range unsafe.Slice((*C.CeMergedStreamItem)(unsafe.Pointer(out.items)), int(out.item_count)) {
			key := []byte{}
			if raw.key_ptr != nil && raw.key_len > 0 {
				key = C.GoBytes(unsafe.Pointer(raw.key_ptr), C.int(raw.key_len))
			}
			tombstone := uint8(raw.value_kind) == valueTombstone
			var value []byte
			if !tombstone {
				if raw.value_ptr != nil && raw.value_len > 0 {
					value = C.GoBytes(unsafe.Pointer(raw.value_ptr), C.int(raw.value_len))
				} else {
					value = []byte{}
				}
			}
			items = append(items, MergedReplayItem{
				Seqno:        uint64(raw.seqno),
				SourceCursor: Cursor{ShardID: uint16(raw.source_shard_id), WALSegmentID: uint32(raw.source_wal_segment_id), WALOffset: uint64(raw.source_wal_offset)},
				Key:          key,
				Value:        value,
				Tombstone:    tombstone,
				RecordType:   uint8(raw.record_type),
				ValueKind:    uint8(raw.value_kind),
				Durability:   uint8(raw.durability),
			})
		}
	}
	return items, next, nil
}

func sameCursorSet(a, b []Cursor) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (e *Engine) TailAllMerged(start []Cursor, pageSize int, pollInterval time.Duration, idleRoundLimit int) ([]MergedReplayItem, []Cursor, error) {
	cursors := append([]Cursor(nil), start...)
	var out []MergedReplayItem
	idleRounds := 0

	for {
		items, next, err := e.ReadFromAllMerged(cursors, pageSize)
		if err != nil {
			return nil, nil, err
		}
		if len(items) > 0 {
			idleRounds = 0
			if sameCursorSet(cursors, next) {
				return nil, nil, fmt.Errorf("tail all merged returned non-advancing cursors")
			}
			out = append(out, items...)
			cursors = next
			if idleRoundLimit > 0 && len(out) >= idleRoundLimit {
				return out, cursors, nil
			}
			continue
		}
		idleRounds++
		if idleRoundLimit > 0 && idleRounds >= idleRoundLimit {
			return out, cursors, nil
		}
		time.Sleep(pollInterval)
	}
}

func checkpointFromC(out C.CeCheckpoint) *Checkpoint {
	cp := &Checkpoint{
		Version:         uint32(out.version),
		ShardCount:      uint16(out.shard_count),
		Kind:            uint8(out.kind),
		CreatedAtUnixNs: uint64(out.created_at_unix_ns),
		HighSeqno:       uint64(out.high_seqno),
	}
	if out.cursors != nil && out.cursor_count > 0 {
		for _, c := range unsafe.Slice((*C.CeCursor)(unsafe.Pointer(out.cursors)), int(out.cursor_count)) {
			cp.Cursors = append(cp.Cursors, toCursor(c))
		}
	}
	return cp
}

func (e *Engine) CaptureCheckpoint(durable bool) (*Checkpoint, error) {
	var out C.CeCheckpoint
	kind := C.CeCheckpointKind(C.CE_CHECKPOINT_DURABLE)
	if !durable {
		kind = C.CeCheckpointKind(C.CE_CHECKPOINT_VISIBLE)
	}
	status := C.ce_capture_checkpoint(e.handle, kind, &out)
	if err := statusErr(status); err != nil {
		return nil, err
	}
	defer C.ce_checkpoint_free(&out)
	return checkpointFromC(out), nil
}

func (e *Engine) SaveCheckpoint(path string, durable bool) (*Checkpoint, error) {
	var out C.CeCheckpoint
	kind := C.CeCheckpointKind(C.CE_CHECKPOINT_DURABLE)
	if !durable {
		kind = C.CeCheckpointKind(C.CE_CHECKPOINT_VISIBLE)
	}
	status := C.ce_capture_checkpoint(e.handle, kind, &out)
	if err := statusErr(status); err != nil {
		return nil, err
	}
	defer C.ce_checkpoint_free(&out)

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	if err := statusErr(C.ce_checkpoint_write_file(&out, cPath)); err != nil {
		return nil, err
	}
	return checkpointFromC(out), nil
}

func LoadCheckpoint(path string) (*Checkpoint, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var out C.CeCheckpoint
	if err := statusErr(C.ce_checkpoint_read_file(cPath, &out)); err != nil {
		return nil, err
	}
	defer C.ce_checkpoint_free(&out)
	return checkpointFromC(out), nil
}

func (e *Engine) ReadFromCheckpoint(cp *Checkpoint, limit int) ([]MergedReplayItem, []Cursor, error) {
	if cp == nil {
		return nil, nil, errors.New("nil checkpoint")
	}
	if cp.Kind == 1 {
		return e.ReadFromAllMergedDurable(cp.Cursors, limit)
	}
	return e.ReadFromAllMerged(cp.Cursors, limit)
}

func (e *Engine) ReadFromCheckpointEx(cp *Checkpoint, limit int) ([]MergedReplayItem, *Checkpoint, error) {
	items, next, err := e.ReadFromCheckpoint(cp, limit)
	if err != nil {
		return nil, nil, err
	}

	nextCP := &Checkpoint{
		Version:         cp.Version,
		ShardCount:      cp.ShardCount,
		Kind:            cp.Kind,
		CreatedAtUnixNs: cp.CreatedAtUnixNs,
		HighSeqno:       cp.HighSeqno,
		Cursors:         next,
	}

	return items, nextCP, nil
}

func (e *Engine) TailFromCheckpoint(
	cp *Checkpoint,
	pageSize int,
	pollInterval time.Duration,
	idleRoundLimit int,
) ([]MergedReplayItem, *Checkpoint, error) {
	if cp == nil {
		return nil, nil, errors.New("nil checkpoint")
	}

	current := &Checkpoint{
		Version:         cp.Version,
		ShardCount:      cp.ShardCount,
		Kind:            cp.Kind,
		CreatedAtUnixNs: cp.CreatedAtUnixNs,
		HighSeqno:       cp.HighSeqno,
		Cursors:         append([]Cursor(nil), cp.Cursors...),
	}

	var out []MergedReplayItem
	idleRounds := 0

	for {
		items, nextCP, err := e.ReadFromCheckpointEx(current, pageSize)
		if err != nil {
			return nil, nil, err
		}

		if len(items) > 0 {
			idleRounds = 0
			out = append(out, items...)
			current = nextCP

			if idleRoundLimit > 0 && len(out) >= idleRoundLimit {
				return out, current, nil
			}
			continue
		}

		idleRounds++
		if idleRoundLimit > 0 && idleRounds >= idleRoundLimit {
			return out, current, nil
		}

		time.Sleep(pollInterval)
	}
}

func SaveCheckpointDataAtomic(path string, cp *Checkpoint) error {
	tmp := path + ".tmp"
	if err := SaveCheckpointData(tmp, cp); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func SaveCheckpointData(path string, cp *Checkpoint) error {
	if cp == nil {
		return errors.New("nil checkpoint")
	}

	var raw C.CeCheckpoint
	raw.version = C.uint32_t(cp.Version)
	raw.shard_count = C.uint16_t(cp.ShardCount)
	raw.kind = C.uint8_t(cp.Kind)
	raw.created_at_unix_ns = C.uint64_t(cp.CreatedAtUnixNs)
	raw.high_seqno = C.uint64_t(cp.HighSeqno)

	var cursors []C.CeCursor
	if len(cp.Cursors) > 0 {
		cursors = make([]C.CeCursor, len(cp.Cursors))
		for i, c := range cp.Cursors {
			cursors[i] = toCCursor(c)
		}
		raw.cursors = (*C.CeCursor)(unsafe.Pointer(&cursors[0]))
		raw.cursor_count = C.size_t(len(cursors))
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	return statusErr(C.ce_checkpoint_write_file(&raw, cPath))
}
