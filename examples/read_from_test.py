import socket
import struct

HOST = "127.0.0.1"
PORT = 7001

def recv_exact(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise RuntimeError("connection closed")
        data += chunk
    return data

def send_read_from(shard_id=0, seg_id=0, offset=0, limit=16, request_id=77):
    payload = (
        struct.pack(">H", shard_id) +
        struct.pack(">H", 0) +
        struct.pack(">I", seg_id) +
        struct.pack(">Q", offset) +
        struct.pack(">I", limit) +
        struct.pack(">I", 0)
    )
    header = struct.pack(">IHHIHH", len(payload), 1, 0x0004, request_id, 0, 0)

    with socket.create_connection((HOST, PORT)) as s:
        s.sendall(header + payload)

        hdr = recv_exact(s, 16)
        frame_len, version, opcode, request_id, flags, status = struct.unpack(">IHHIHH", hdr)
        payload = recv_exact(s, frame_len)

        print("response:")
        print("  frame_len :", frame_len)
        print("  version   :", version)
        print("  opcode    :", opcode)
        print("  request_id:", request_id)
        print("  flags     :", flags)
        print("  status    :", status)

        if status != 0:
            print("error status")
            return

        next_shard = struct.unpack(">H", payload[0:2])[0]
        next_seg = struct.unpack(">I", payload[4:8])[0]
        next_off = struct.unpack(">Q", payload[8:16])[0]
        item_count = struct.unpack(">I", payload[16:20])[0]

        print("  next_shard:", next_shard)
        print("  next_seg  :", next_seg)
        print("  next_off  :", next_off)
        print("  item_count:", item_count)

        off = 24
        for i in range(item_count):
            seqno = struct.unpack(">Q", payload[off:off+8])[0]
            durability = payload[off+8]
            record_type = payload[off+9]
            value_kind = payload[off+10]
            key_len = struct.unpack(">H", payload[off+12:off+14])[0]
            value_len = struct.unpack(">I", payload[off+16:off+20])[0]
            off += 20

            key = payload[off:off+key_len]
            off += key_len

            value = payload[off:off+value_len]
            off += value_len

            print(f"  item[{i}]")
            print("    seqno      :", seqno)
            print("    durability :", durability)
            print("    record_type:", record_type)
            print("    value_kind :", value_kind)
            print("    key        :", key)
            print("    value      :", value)

send_read_from(shard_id=0, seg_id=0, offset=0, limit=16, request_id=77)
send_read_from(shard_id=1, seg_id=0, offset=0, limit=16, request_id=78)
send_read_from(shard_id=2, seg_id=0, offset=0, limit=16, request_id=79)
send_read_from(shard_id=3, seg_id=0, offset=0, limit=16, request_id=80)