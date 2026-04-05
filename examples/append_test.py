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

def send_append(key: bytes, value: bytes, durability=0, request_id=100):
    payload = (
        struct.pack(">BBHI", durability, 0, len(key), len(value))
        + key
        + value
    )
    header = struct.pack(">IHHIHH", len(payload), 1, 0x0002, request_id, 0, 0)

    with socket.create_connection((HOST, PORT)) as s:
        s.sendall(header + payload)

        hdr = recv_exact(s, 16)
        frame_len, version, opcode, request_id, flags, status = struct.unpack(">IHHIHH", hdr)
        payload = recv_exact(s, frame_len)

        print("append response:")
        print("  frame_len :", frame_len)
        print("  version   :", version)
        print("  opcode    :", opcode)
        print("  request_id:", request_id)
        print("  flags     :", flags)
        print("  status    :", status)
        print("  payload   :", payload)

        if len(payload) == 8:
            commit_token = struct.unpack(">Q", payload)[0]
            print("  commit_token:", commit_token)

send_append(b"user:1", b"Alice", durability=0, request_id=100)