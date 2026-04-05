import socket
import struct

HOST = "127.0.0.1"
PORT = 7001

# header:
# u32 frame_len
# u16 version
# u16 opcode
# u32 request_id
# u16 flags
# u16 status

req = struct.pack(">IHHIHH", 0, 1, 0x0001, 123, 0, 0)

with socket.create_connection((HOST, PORT)) as s:
    s.sendall(req)

    hdr = s.recv(16)
    if len(hdr) != 16:
        raise RuntimeError(f"short header: {len(hdr)}")

    frame_len, version, opcode, request_id, flags, status = struct.unpack(">IHHIHH", hdr)
    print("response header:")
    print("  frame_len :", frame_len)
    print("  version   :", version)
    print("  opcode    :", opcode)
    print("  request_id:", request_id)
    print("  flags     :", flags)
    print("  status    :", status)

    payload = b""
    while len(payload) < frame_len:
        chunk = s.recv(frame_len - len(payload))
        if not chunk:
            raise RuntimeError("connection closed while reading payload")
        payload += chunk

    print("payload bytes:", payload)

    if len(payload) == 8:
        major, minor, patch, reserved = struct.unpack(">HHHH", payload)
        print("server version:")
        print("  major   :", major)
        print("  minor   :", minor)
        print("  patch   :", patch)
        print("  reserved:", reserved)