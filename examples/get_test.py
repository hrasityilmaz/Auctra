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

def send_get(key: bytes, request_id=1):
    payload = struct.pack(">H", len(key)) + key
    header = struct.pack(">IHHIHH", len(payload), 1, 0x0003, request_id, 0, 0)

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
        print("  payload   :", payload)

        if len(payload) >= 8:
            found = payload[0]
            value_len = struct.unpack(">I", payload[4:8])[0]
            value = payload[8:8+value_len]
            print("  found     :", found)
            print("  value_len :", value_len)
            print("  value     :", value)

send_get(b"user:1", 42)