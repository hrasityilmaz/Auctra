import socket
import struct
from dataclasses import dataclass
from typing import List


PROTOCOL_VERSION = 2

OPCODE_PING = 0x0001
OPCODE_APPEND = 0x0002
OPCODE_GET = 0x0003
OPCODE_READ_FROM = 0x0004
OPCODE_STATS = 0x0005
OPCODE_READ_FROM_ALL_MERGED = 0x0006


STATUS_OK = 0


@dataclass
class ResponseHeader:
    frame_len: int
    version: int
    opcode: int
    request_id: int
    flags: int
    status: int


@dataclass
class Cursor:
    shard_id: int
    wal_segment_id: int
    wal_offset: int


@dataclass
class ShardCursor:
    wal_segment_id: int
    wal_offset: int


@dataclass
class GlobalMergeCursor:
    shard_cursors: List[ShardCursor]


@dataclass
class GetResult:
    found: bool
    value: bytes


@dataclass
class AppendResult:
    seqno: int
    shard_id: int
    wal_segment_id: int
    wal_offset: int
    visible_segment_id: int
    visible_offset: int
    durable_segment_id: int
    durable_offset: int
    record_count: int


@dataclass
class ReadItem:
    seqno: int
    durability: int
    record_type: int
    value_kind: int
    key: bytes
    value: bytes


@dataclass
class ReadFromResult:
    next_cursor: Cursor
    items: List[ReadItem]


@dataclass
class ReadFromAllMergedResult:
    next_cursor: GlobalMergeCursor
    items: List[ReadItem]


class AuctraClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 7001):
        self.sock = socket.create_connection((host, port))
        self._next_request_id = 1

    def close(self) -> None:
        try:
            self.sock.close()
        finally:
            self.sock = None  # type: ignore[assignment]

    def __enter__(self) -> "AuctraClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _next_id(self) -> int:
        rid = self._next_request_id
        self._next_request_id += 1
        return rid

    def _recv_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise EOFError("connection closed")
            buf.extend(chunk)
        return bytes(buf)

    def _write_header(self, opcode: int, request_id: int, payload_len: int) -> None:
        header = struct.pack(
            ">IHHIHH",
            payload_len,
            PROTOCOL_VERSION,
            opcode,
            request_id,
            0,  # flags
            0,  # status
        )
        self.sock.sendall(header)

    def _read_response(self) -> tuple[ResponseHeader, bytes]:
        raw = self._recv_exact(16)
        frame_len, version, opcode, request_id, flags, status = struct.unpack(">IHHIHH", raw)

        payload = b""
        if frame_len > 0:
            payload = self._recv_exact(frame_len)

        return (
            ResponseHeader(
                frame_len=frame_len,
                version=version,
                opcode=opcode,
                request_id=request_id,
                flags=flags,
                status=status,
            ),
            payload,
        )

    def ping(self) -> None:
        request_id = self._next_id()
        self._write_header(OPCODE_PING, request_id, 0)
        hdr, payload = self._read_response()

        if hdr.status != STATUS_OK:
            raise RuntimeError(f"ping failed: status={hdr.status}")

        if len(payload) != 8:
            raise RuntimeError(f"invalid ping payload size: {len(payload)}")

    def append(self, key: bytes, value: bytes, durability: int) -> AppendResult:
        request_id = self._next_id()

        payload = struct.pack(">BBHI", durability, 0, len(key), len(value)) + key + value
        self._write_header(OPCODE_APPEND, request_id, len(payload))
        self.sock.sendall(payload)

        hdr, payload_resp = self._read_response()

        if hdr.status != STATUS_OK:
            raise RuntimeError(f"append failed: status={hdr.status}")

        if len(payload_resp) != 52:
            raise RuntimeError(f"invalid append payload size: {len(payload_resp)}")

        (
            seqno,
            shard_id,
            _reserved,
            wal_segment_id,
            wal_offset,
            visible_segment_id,
            visible_offset,
            durable_segment_id,
            durable_offset,
            record_count,
        ) = struct.unpack(">QHHIQIQIQI", payload_resp)

        return AppendResult(
            seqno=seqno,
            shard_id=shard_id,
            wal_segment_id=wal_segment_id,
            wal_offset=wal_offset,
            visible_segment_id=visible_segment_id,
            visible_offset=visible_offset,
            durable_segment_id=durable_segment_id,
            durable_offset=durable_offset,
            record_count=record_count,
        )

    def get(self, key: bytes) -> GetResult:
        request_id = self._next_id()

        payload = struct.pack(">H", len(key)) + key
        self._write_header(OPCODE_GET, request_id, len(payload))
        self.sock.sendall(payload)

        hdr, payload_resp = self._read_response()

        if hdr.status != STATUS_OK:
            raise RuntimeError(f"get failed: status={hdr.status}")

        if len(payload_resp) < 8:
            raise RuntimeError(f"get payload too short: {len(payload_resp)}")

        found = payload_resp[0] == 1
        value_len = struct.unpack(">I", payload_resp[4:8])[0]

        if len(payload_resp) < 8 + value_len:
            raise RuntimeError("get payload truncated")

        value = payload_resp[8 : 8 + value_len]
        return GetResult(found=found, value=value)

    def stats(self) -> tuple[int, int]:
        request_id = self._next_id()
        self._write_header(OPCODE_STATS, request_id, 0)

        hdr, payload = self._read_response()

        if hdr.status != STATUS_OK:
            raise RuntimeError(f"stats failed: status={hdr.status}")

        if len(payload) != 12:
            raise RuntimeError(f"invalid stats payload size: {len(payload)}")

        shard_count, uptime_seconds = struct.unpack(">IQ", payload)
        return shard_count, uptime_seconds

    def read_from(self, cursor: Cursor, limit: int) -> ReadFromResult:
        request_id = self._next_id()

        payload = struct.pack(
            ">HHIQII",
            cursor.shard_id,
            0,
            cursor.wal_segment_id,
            cursor.wal_offset,
            limit,
            0,
        )

        self._write_header(OPCODE_READ_FROM, request_id, len(payload))
        self.sock.sendall(payload)

        hdr, payload_resp = self._read_response()

        if hdr.status != STATUS_OK:
            raise RuntimeError(f"read_from failed: status={hdr.status}")

        if len(payload_resp) < 24:
            raise RuntimeError(f"read_from payload too short: {len(payload_resp)}")

        shard_id, _reserved, wal_segment_id, wal_offset, item_count, _reserved2 = struct.unpack(
            ">HHIQII", payload_resp[:24]
        )

        off = 24
        items: List[ReadItem] = []

        for _ in range(item_count):
            if len(payload_resp) < off + 20:
                raise RuntimeError("read_from item header truncated")

            seqno = struct.unpack(">Q", payload_resp[off : off + 8])[0]
            durability = payload_resp[off + 8]
            record_type = payload_resp[off + 9]
            value_kind = payload_resp[off + 10]
            key_len = struct.unpack(">H", payload_resp[off + 12 : off + 14])[0]
            value_len = struct.unpack(">I", payload_resp[off + 16 : off + 20])[0]
            off += 20

            if len(payload_resp) < off + key_len + value_len:
                raise RuntimeError("read_from item payload truncated")

            key = payload_resp[off : off + key_len]
            off += key_len

            value = payload_resp[off : off + value_len]
            off += value_len

            items.append(
                ReadItem(
                    seqno=seqno,
                    durability=durability,
                    record_type=record_type,
                    value_kind=value_kind,
                    key=key,
                    value=value,
                )
            )

        return ReadFromResult(
            next_cursor=Cursor(
                shard_id=shard_id,
                wal_segment_id=wal_segment_id,
                wal_offset=wal_offset,
            ),
            items=items,
        )

    def read_from_all_merged(
        self, cursor: GlobalMergeCursor, limit: int
    ) -> ReadFromAllMergedResult:
        request_id = self._next_id()

        shard_count = len(cursor.shard_cursors)
        payload = struct.pack(">HHII", shard_count, 0, limit, 0)

        for sc in cursor.shard_cursors:
            payload += struct.pack(">IQ", sc.wal_segment_id, sc.wal_offset)

        self._write_header(OPCODE_READ_FROM_ALL_MERGED, request_id, len(payload))
        self.sock.sendall(payload)

        hdr, payload_resp = self._read_response()

        if hdr.status != STATUS_OK:
            raise RuntimeError(f"read_from_all_merged failed: status={hdr.status}")

        if len(payload_resp) < 12:
            raise RuntimeError(f"merged payload too short: {len(payload_resp)}")

        resp_shard_count, _reserved, item_count, _reserved2 = struct.unpack(">HHII", payload_resp[:12])

        off = 12
        shard_cursors: List[ShardCursor] = []

        for _ in range(resp_shard_count):
            if len(payload_resp) < off + 12:
                raise RuntimeError("merged cursor payload truncated")

            wal_segment_id, wal_offset = struct.unpack(">IQ", payload_resp[off : off + 12])
            off += 12
            shard_cursors.append(
                ShardCursor(
                    wal_segment_id=wal_segment_id,
                    wal_offset=wal_offset,
                )
            )

        items: List[ReadItem] = []

        for _ in range(item_count):
            if len(payload_resp) < off + 20:
                raise RuntimeError("merged item header truncated")

            seqno = struct.unpack(">Q", payload_resp[off : off + 8])[0]
            durability = payload_resp[off + 8]
            record_type = payload_resp[off + 9]
            value_kind = payload_resp[off + 10]
            key_len = struct.unpack(">H", payload_resp[off + 12 : off + 14])[0]
            value_len = struct.unpack(">I", payload_resp[off + 16 : off + 20])[0]
            off += 20

            if len(payload_resp) < off + key_len + value_len:
                raise RuntimeError("merged item payload truncated")

            key = payload_resp[off : off + key_len]
            off += key_len

            value = payload_resp[off : off + value_len]
            off += value_len

            items.append(
                ReadItem(
                    seqno=seqno,
                    durability=durability,
                    record_type=record_type,
                    value_kind=value_kind,
                    key=key,
                    value=value,
                )
            )

        return ReadFromAllMergedResult(
            next_cursor=GlobalMergeCursor(shard_cursors=shard_cursors),
            items=items,
        )