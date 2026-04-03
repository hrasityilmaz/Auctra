
import ctypes
from dataclasses import dataclass
from pathlib import Path


class Cursor(ctypes.Structure):
    _fields_ = [
        ("shard_id", ctypes.c_uint16),
        ("wal_segment_id", ctypes.c_uint32),
        ("wal_offset", ctypes.c_uint64),
    ]


class CommitWindow(ctypes.Structure):
    _fields_ = [
        ("shard_id", ctypes.c_uint16),
        ("start", Cursor),
        ("end", Cursor),
        ("visible", Cursor),
        ("durable", Cursor),
        ("has_durable", ctypes.c_uint8),
        ("record_count", ctypes.c_size_t),
        ("first_seqno", ctypes.c_uint64),
        ("last_seqno", ctypes.c_uint64),
    ]


class Buffer(ctypes.Structure):
    _fields_ = [
        ("ptr", ctypes.POINTER(ctypes.c_uint8)),
        ("len", ctypes.c_size_t),
    ]


class StreamItem(ctypes.Structure):
    _fields_ = [
        ("seqno", ctypes.c_uint64),

        ("cursor_shard_id", ctypes.c_uint16),
        ("reserved0", ctypes.c_uint16),
        ("cursor_wal_segment_id", ctypes.c_uint32),
        ("cursor_wal_offset", ctypes.c_uint64),

        ("record_type", ctypes.c_uint8),
        ("value_kind", ctypes.c_uint8),
        ("durability", ctypes.c_uint8),
        ("reserved1", ctypes.c_uint8),

        ("key_ptr", ctypes.POINTER(ctypes.c_uint8)),
        ("key_len", ctypes.c_size_t),
        ("value_ptr", ctypes.POINTER(ctypes.c_uint8)),
        ("value_len", ctypes.c_size_t),
    ]


class ReadResult(ctypes.Structure):
    _fields_ = [
        ("items", ctypes.POINTER(StreamItem)),
        ("item_count", ctypes.c_size_t),

        ("next_shard_id", ctypes.c_uint16),
        ("reserved0", ctypes.c_uint16),
        ("next_wal_segment_id", ctypes.c_uint32),
        ("next_wal_offset", ctypes.c_uint64),

        ("internal", ctypes.c_void_p),
    ]


class MergedStreamItem(ctypes.Structure):
    _fields_ = [
        ("seqno", ctypes.c_uint64),

        ("source_shard_id", ctypes.c_uint16),
        ("reserved0", ctypes.c_uint16),
        ("source_wal_segment_id", ctypes.c_uint32),
        ("source_wal_offset", ctypes.c_uint64),

        ("record_type", ctypes.c_uint8),
        ("value_kind", ctypes.c_uint8),
        ("durability", ctypes.c_uint8),
        ("reserved1", ctypes.c_uint8),

        ("key_ptr", ctypes.POINTER(ctypes.c_uint8)),
        ("key_len", ctypes.c_size_t),
        ("value_ptr", ctypes.POINTER(ctypes.c_uint8)),
        ("value_len", ctypes.c_size_t),
    ]


class MergedReadResult(ctypes.Structure):
    _fields_ = [
        ("items", ctypes.POINTER(MergedStreamItem)),
        ("item_count", ctypes.c_size_t),
        ("next_cursors", ctypes.POINTER(Cursor)),
        ("next_cursor_count", ctypes.c_size_t),
        ("internal", ctypes.c_void_p),
    ]

class Checkpoint(ctypes.Structure):
    _fields_ = [
        ("version", ctypes.c_uint32),
        ("shard_count", ctypes.c_uint16),
        ("reserved0", ctypes.c_uint16),
        ("kind", ctypes.c_uint8),
        ("reserved1", ctypes.c_uint8 * 7),
        ("created_at_unix_ns", ctypes.c_uint64),
        ("high_seqno", ctypes.c_uint64),
        ("cursors", ctypes.POINTER(Cursor)),
        ("cursor_count", ctypes.c_size_t),
        ("internal", ctypes.c_void_p),
    ]


CE_READ_VISIBLE = 0
CE_READ_DURABLE = 1

CE_RECORD_PUT_INLINE = 0
CE_RECORD_PUT_BLOB = 1
CE_RECORD_TOMBSTONE = 2

CE_VALUE_INLINE = 0
CE_VALUE_BLOB = 1
CE_VALUE_TOMBSTONE = 2

CE_DURABILITY_ULTRAFAST = 0
CE_DURABILITY_BATCH = 1
CE_DURABILITY_STRICT = 2


@dataclass
class ReplayItem:
    seqno: int
    cursor: Cursor
    key: bytes
    value: bytes | None
    record_type: str
    value_kind: str
    durability: str


@dataclass
class MergedReplayItem:
    seqno: int
    source_cursor: Cursor
    key: bytes
    value: bytes | None
    record_type: str
    value_kind: str
    durability: str

@dataclass
class CheckpointData:
    version: int
    shard_count: int
    kind: str
    created_at_unix_ns: int
    high_seqno: int
    cursors: list[Cursor]


def _record_type_name(value: int) -> str:
    return {
        CE_RECORD_PUT_INLINE: "put_inline",
        CE_RECORD_PUT_BLOB: "put_blob",
        CE_RECORD_TOMBSTONE: "tombstone",
    }.get(value, f"unknown({value})")


def _value_kind_name(value: int) -> str:
    return {
        CE_VALUE_INLINE: "inline",
        CE_VALUE_BLOB: "blob",
        CE_VALUE_TOMBSTONE: "tombstone",
    }.get(value, f"unknown({value})")


def _durability_name(value: int) -> str:
    return {
        CE_DURABILITY_ULTRAFAST: "ultrafast",
        CE_DURABILITY_BATCH: "batch",
        CE_DURABILITY_STRICT: "strict",
    }.get(value, f"unknown({value})")


def _same_cursor_set(a: list[Cursor], b: list[Cursor]) -> bool:
    if len(a) != len(b):
        return False
    for x, y in zip(a, b):
        if (
            x.shard_id != y.shard_id
            or x.wal_segment_id != y.wal_segment_id
            or x.wal_offset != y.wal_offset
        ):
            return False
    return True


class CommitEngine:
    def __init__(self, lib_path: str | None = None, shard_count: int = 4, inline_max: int = 1024):
        if lib_path is None:
            root = Path(__file__).resolve().parents[2]
            lib_path = root / "zig-out" / "lib" / "libcommit_engine.so"

        self.lib = ctypes.CDLL(str(lib_path))
        self._bind()

        self.handle = self.lib.ce_engine_open(shard_count, inline_max)
        if not self.handle:
            raise RuntimeError("failed to open commit engine")

    def _bind(self) -> None:
        self.lib.ce_engine_open.argtypes = [ctypes.c_uint16, ctypes.c_uint32]
        self.lib.ce_engine_open.restype = ctypes.c_void_p

        self.lib.ce_engine_open_default.argtypes = []
        self.lib.ce_engine_open_default.restype = ctypes.c_void_p

        self.lib.ce_engine_close.argtypes = [ctypes.c_void_p]
        self.lib.ce_engine_close.restype = None

        self.lib.ce_put.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_uint8,
            ctypes.POINTER(CommitWindow),
        ]
        self.lib.ce_put.restype = ctypes.c_int

        self.lib.ce_delete.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_uint8,
            ctypes.POINTER(CommitWindow),
        ]
        self.lib.ce_delete.restype = ctypes.c_int

        self.lib.ce_get.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.POINTER(Buffer),
        ]
        self.lib.ce_get.restype = ctypes.c_int

        self.lib.ce_get_at.argtypes = [
            ctypes.c_void_p,
            Cursor,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.POINTER(Buffer),
        ]
        self.lib.ce_get_at.restype = ctypes.c_int

        self.lib.ce_read_from.argtypes = [
            ctypes.c_void_p,
            Cursor,
            ctypes.c_size_t,
            ctypes.POINTER(ReadResult),
        ]
        self.lib.ce_read_from.restype = ctypes.c_int

        self.lib.ce_read_from_consistent.argtypes = [
            ctypes.c_void_p,
            Cursor,
            ctypes.c_size_t,
            ctypes.c_int,
            ctypes.POINTER(ReadResult),
        ]
        self.lib.ce_read_from_consistent.restype = ctypes.c_int

        self.lib.ce_read_result_free.argtypes = [ctypes.POINTER(ReadResult)]
        self.lib.ce_read_result_free.restype = None

        self.lib.ce_read_from_all_merged.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(Cursor),
            ctypes.c_size_t,
            ctypes.c_size_t,
            ctypes.POINTER(MergedReadResult),
        ]
        self.lib.ce_read_from_all_merged.restype = ctypes.c_int

        self.lib.ce_read_from_all_merged_consistent.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(Cursor),
            ctypes.c_size_t,
            ctypes.c_size_t,
            ctypes.c_int,
            ctypes.POINTER(MergedReadResult),
        ]
        self.lib.ce_read_from_all_merged_consistent.restype = ctypes.c_int

        self.lib.ce_merged_read_result_free.argtypes = [ctypes.POINTER(MergedReadResult)]
        self.lib.ce_merged_read_result_free.restype = None

        self.lib.ce_capture_checkpoint.argtypes = [
            ctypes.c_void_p,
            ctypes.c_int,
            ctypes.POINTER(Checkpoint),
        ]
        self.lib.ce_capture_checkpoint.restype = ctypes.c_int

        self.lib.ce_checkpoint_write_file.argtypes = [ctypes.POINTER(Checkpoint), ctypes.c_char_p]
        self.lib.ce_checkpoint_write_file.restype = ctypes.c_int

        self.lib.ce_checkpoint_read_file.argtypes = [ctypes.c_char_p, ctypes.POINTER(Checkpoint)]
        self.lib.ce_checkpoint_read_file.restype = ctypes.c_int

        self.lib.ce_checkpoint_free.argtypes = [ctypes.POINTER(Checkpoint)]
        self.lib.ce_checkpoint_free.restype = None

        self.lib.ce_checkpoint_to_merged_cursors.argtypes = [
            ctypes.POINTER(Checkpoint),
            ctypes.POINTER(ctypes.POINTER(Cursor)),
            ctypes.POINTER(ctypes.c_size_t),
        ]
        self.lib.ce_checkpoint_to_merged_cursors.restype = ctypes.c_int

        self.lib.ce_checkpoint_cursor_array_free.argtypes = [ctypes.POINTER(Cursor), ctypes.c_size_t]
        self.lib.ce_checkpoint_cursor_array_free.restype = None

        self.lib.ce_buffer_free.argtypes = [ctypes.POINTER(Buffer)]
        self.lib.ce_buffer_free.restype = None

        self.lib.ce_status_string.argtypes = [ctypes.c_int]
        self.lib.ce_status_string.restype = ctypes.c_char_p

    def close(self) -> None:
        if self.handle:
            self.lib.ce_engine_close(self.handle)
            self.handle = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def _check(self, status: int) -> None:
        if status != 0:
            msg = self.lib.ce_status_string(status).decode("utf-8")
            raise RuntimeError(f"commit engine error: {msg} ({status})")

    @staticmethod
    def _raw(data: bytes):
        buf = ctypes.create_string_buffer(data)
        return buf, ctypes.cast(buf, ctypes.c_void_p), len(data)

    def put(self, key: bytes, value: bytes, await_durable: bool = True) -> CommitWindow:
        key_buf, key_ptr, key_len = self._raw(key)
        value_buf, value_ptr, value_len = self._raw(value)
        window = CommitWindow()
        status = self.lib.ce_put(
            self.handle,
            key_ptr,
            key_len,
            value_ptr,
            value_len,
            1 if await_durable else 0,
            ctypes.byref(window),
        )
        self._check(status)
        return window

    def delete(self, key: bytes, await_durable: bool = True) -> CommitWindow:
        key_buf, key_ptr, key_len = self._raw(key)
        window = CommitWindow()
        status = self.lib.ce_delete(
            self.handle,
            key_ptr,
            key_len,
            1 if await_durable else 0,
            ctypes.byref(window),
        )
        self._check(status)
        return window

    def get(self, key: bytes) -> bytes | None:
        key_buf, key_ptr, key_len = self._raw(key)
        out = Buffer()
        status = self.lib.ce_get(self.handle, key_ptr, key_len, ctypes.byref(out))
        if status == 4:
            return None
        self._check(status)
        try:
            if not out.ptr:
                return b""
            return ctypes.string_at(out.ptr, out.len)
        finally:
            self.lib.ce_buffer_free(ctypes.byref(out))

    def get_at(self, cursor: Cursor, key: bytes) -> bytes | None:
        key_buf, key_ptr, key_len = self._raw(key)
        out = Buffer()
        status = self.lib.ce_get_at(self.handle, cursor, key_ptr, key_len, ctypes.byref(out))
        if status == 4:
            return None
        self._check(status)
        try:
            if not out.ptr:
                return b""
            return ctypes.string_at(out.ptr, out.len)
        finally:
            self.lib.ce_buffer_free(ctypes.byref(out))

    def read_from(
        self,
        start: Cursor,
        limit: int = 256,
        durable: bool = False,
    ) -> tuple[list[ReplayItem], Cursor]:
        out = ReadResult()
        status = self.lib.ce_read_from_consistent(
            self.handle,
            start,
            limit,
            CE_READ_DURABLE if durable else CE_READ_VISIBLE,
            ctypes.byref(out),
        )
        self._check(status)

        try:
            next_cursor = Cursor(
                shard_id=out.next_shard_id,
                wal_segment_id=out.next_wal_segment_id,
                wal_offset=out.next_wal_offset,
            )

            items: list[ReplayItem] = []
            for i in range(out.item_count):
                raw = out.items[i]

                item_cursor = Cursor(
                    shard_id=raw.cursor_shard_id,
                    wal_segment_id=raw.cursor_wal_segment_id,
                    wal_offset=raw.cursor_wal_offset,
                )

                key = ctypes.string_at(raw.key_ptr, raw.key_len) if raw.key_ptr else b""
                value = None
                if raw.value_kind != CE_VALUE_TOMBSTONE:
                    value = ctypes.string_at(raw.value_ptr, raw.value_len) if raw.value_ptr else b""

                items.append(
                    ReplayItem(
                        seqno=raw.seqno,
                        cursor=item_cursor,
                        key=key,
                        value=value,
                        record_type=_record_type_name(raw.record_type),
                        value_kind=_value_kind_name(raw.value_kind),
                        durability=_durability_name(raw.durability),
                    )
                )

            return items, next_cursor
        finally:
            self.lib.ce_read_result_free(ctypes.byref(out))

    def replay(
        self,
        start: Cursor,
        page_size: int = 256,
        durable: bool = False,
    ):
        cursor = start
        while True:
            items, next_cursor = self.read_from(cursor, page_size, durable=durable)

            if not items:
                break

            if (
                next_cursor.shard_id == cursor.shard_id
                and next_cursor.wal_segment_id == cursor.wal_segment_id
                and next_cursor.wal_offset == cursor.wal_offset
            ):
                raise RuntimeError(
                    f"read_from returned a non-advancing cursor: "
                    f"({cursor.shard_id}, {cursor.wal_segment_id}, {cursor.wal_offset})"
                )

            for item in items:
                yield item

            cursor = next_cursor

    def read_from_all_merged(
        self,
        start_cursors: list[Cursor],
        limit: int = 256,
        durable: bool = False,
    ) -> tuple[list[MergedReplayItem], list[Cursor]]:
        if not start_cursors:
            raise ValueError("start_cursors must not be empty")

        cursor_array = (Cursor * len(start_cursors))(*start_cursors)
        out = MergedReadResult()

        status = self.lib.ce_read_from_all_merged_consistent(
            self.handle,
            cursor_array,
            len(start_cursors),
            limit,
            CE_READ_DURABLE if durable else CE_READ_VISIBLE,
            ctypes.byref(out),
        )
        self._check(status)

        try:
            next_cursors = [out.next_cursors[i] for i in range(out.next_cursor_count)]

            items: list[MergedReplayItem] = []
            for i in range(out.item_count):
                raw = out.items[i]

                source_cursor = Cursor(
                    shard_id=raw.source_shard_id,
                    wal_segment_id=raw.source_wal_segment_id,
                    wal_offset=raw.source_wal_offset,
                )

                key = ctypes.string_at(raw.key_ptr, raw.key_len) if raw.key_ptr else b""
                value = None
                if raw.value_kind != CE_VALUE_TOMBSTONE:
                    value = ctypes.string_at(raw.value_ptr, raw.value_len) if raw.value_ptr else b""

                items.append(
                    MergedReplayItem(
                        seqno=raw.seqno,
                        source_cursor=source_cursor,
                        key=key,
                        value=value,
                        record_type=_record_type_name(raw.record_type),
                        value_kind=_value_kind_name(raw.value_kind),
                        durability=_durability_name(raw.durability),
                    )
                )

            return items, next_cursors
        finally:
            self.lib.ce_merged_read_result_free(ctypes.byref(out))

    def replay_all_merged(
        self,
        start_cursors: list[Cursor],
        page_size: int = 256,
        durable: bool = False,
    ):
        cursors = start_cursors
        while True:
            items, next_cursors = self.read_from_all_merged(
                cursors,
                limit=page_size,
                durable=durable,
            )

            if not items:
                break

            if _same_cursor_set(cursors, next_cursors):
                raise RuntimeError("merged replay returned non-advancing cursors")

            for item in items:
                yield item

            cursors = next_cursors


    @staticmethod
    def _checkpoint_kind_name(value: int) -> str:
        return {0: "visible", 1: "durable"}.get(value, f"unknown({value})")

    @staticmethod
    def _clone_cursor(raw: Cursor) -> Cursor:
        return Cursor(
            shard_id=int(raw.shard_id),
            wal_segment_id=int(raw.wal_segment_id),
            wal_offset=int(raw.wal_offset),
        )

    def _checkpoint_to_data(self, cp: Checkpoint) -> CheckpointData:
        cursors = [self._clone_cursor(cp.cursors[i]) for i in range(cp.cursor_count)]
        return CheckpointData(
            version=int(cp.version),
            shard_count=int(cp.shard_count),
            kind=self._checkpoint_kind_name(int(cp.kind)),
            created_at_unix_ns=int(cp.created_at_unix_ns),
            high_seqno=int(cp.high_seqno),
            cursors=cursors,
        )

    def capture_checkpoint(self, durable: bool = True) -> CheckpointData:
        cp = Checkpoint()
        status = self.lib.ce_capture_checkpoint(self.handle, 1 if durable else 0, ctypes.byref(cp))
        self._check(status)
        try:
            return self._checkpoint_to_data(cp)
        finally:
            self.lib.ce_checkpoint_free(ctypes.byref(cp))

    def save_checkpoint(self, path: str, durable: bool = True) -> CheckpointData:
        cp = Checkpoint()
        status = self.lib.ce_capture_checkpoint(self.handle, 1 if durable else 0, ctypes.byref(cp))
        self._check(status)
        try:
            self._check(self.lib.ce_checkpoint_write_file(ctypes.byref(cp), str(path).encode("utf-8")))
            return self._checkpoint_to_data(cp)
        finally:
            self.lib.ce_checkpoint_free(ctypes.byref(cp))

    def load_checkpoint(self, path: str) -> CheckpointData:
        cp = Checkpoint()
        self._check(self.lib.ce_checkpoint_read_file(str(path).encode("utf-8"), ctypes.byref(cp)))
        try:
            return self._checkpoint_to_data(cp)
        finally:
            self.lib.ce_checkpoint_free(ctypes.byref(cp))

    def read_from_checkpoint(self, checkpoint: CheckpointData, limit: int = 256):
        items, next_cursors = self.read_from_all_merged(
            checkpoint.cursors,
            limit=limit,
            durable=checkpoint.kind == "durable",
        )

        next_checkpoint = CheckpointData(
            version=checkpoint.version,
            shard_count=checkpoint.shard_count,
            kind=checkpoint.kind,
            created_at_unix_ns=checkpoint.created_at_unix_ns,
            high_seqno=checkpoint.high_seqno,
            cursors=next_cursors,
        )

        return items, next_checkpoint
    
    
    def save_checkpoint_data(self, path: str, checkpoint: CheckpointData) -> None:
        cp = Checkpoint()
        cp.version = checkpoint.version
        cp.shard_count = checkpoint.shard_count
        cp.reserved0 = 0
        cp.kind = 1 if checkpoint.kind == "durable" else 0
        cp.created_at_unix_ns = checkpoint.created_at_unix_ns
        cp.high_seqno = checkpoint.high_seqno

        cursor_array = (Cursor * len(checkpoint.cursors))(
            *[
                Cursor(
                    shard_id=c.shard_id,
                    wal_segment_id=c.wal_segment_id,
                    wal_offset=c.wal_offset,
                )
                for c in checkpoint.cursors
            ]
        )
        cp.cursors = cursor_array
        cp.cursor_count = len(checkpoint.cursors)
        cp.internal = None

        self._check(self.lib.ce_checkpoint_write_file(ctypes.byref(cp), str(path).encode("utf-8")))

    def tail_from_checkpoint(
        self,
        checkpoint: CheckpointData,
        page_size: int = 256,
        poll_interval: float = 0.1,
        idle_round_limit: int = 0,
        ):
        import time

        current = CheckpointData(
            version=checkpoint.version,
            shard_count=checkpoint.shard_count,
            kind=checkpoint.kind,
            created_at_unix_ns=checkpoint.created_at_unix_ns,
            high_seqno=checkpoint.high_seqno,
            cursors=list(checkpoint.cursors),
        )

        idle_rounds = 0

        while True:
            items, next_checkpoint = self.read_from_checkpoint(current, limit=page_size)

            if items:
                idle_rounds = 0
                yield items, next_checkpoint
                current = next_checkpoint
                continue

            idle_rounds += 1
            if idle_round_limit > 0 and idle_rounds >= idle_round_limit:
                return

            time.sleep(poll_interval)
    
    def save_checkpoint_data_atomic(self, path: str, checkpoint: CheckpointData) -> None:
        import os
        tmp = path + ".tmp"
        self.save_checkpoint_data(tmp, checkpoint)
        os.replace(tmp, path)