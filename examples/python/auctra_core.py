import socket
import struct

VERSION = 1
MAX_FRAME = 16 * 1024 * 1024


class AuctraClient:
    def __init__(self, host="127.0.0.1", port=7001, timeout=5):
        self.sock = socket.create_connection((host, port))
        self.sock.settimeout(timeout)

    def close(self):
        self.sock.close()

    def _read_exact(self, n):
        data = b""
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise RuntimeError("connection closed")
            data += chunk
        return data

    def _validate(self, resp, expected_opcode):
        version = resp[0]
        opcode = resp[1]

        if version != VERSION:
            raise RuntimeError("version mismatch")

        if opcode != expected_opcode:
            raise RuntimeError("opcode mismatch")

    def ping(self):
        header = struct.pack("BB", VERSION, 1)

        self.sock.sendall(header)

        resp = self._read_exact(8)
        self._validate(resp, 1)