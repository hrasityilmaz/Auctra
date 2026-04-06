package main

import (
	"encoding/binary"
	"fmt"
	"net"
)

const (
	OpcodePing     = 0x0001
	OpcodeAppend   = 0x0002
	OpcodeGet      = 0x0003
	OpcodeReadFrom = 0x0004
)

func writeHeader(conn net.Conn, opcode uint16, requestID uint32, payloadLen uint32) error {
	buf := make([]byte, 16)

	binary.BigEndian.PutUint32(buf[0:4], payloadLen)
	binary.BigEndian.PutUint16(buf[4:6], 2)
	binary.BigEndian.PutUint16(buf[6:8], opcode)
	binary.BigEndian.PutUint32(buf[8:12], requestID)
	binary.BigEndian.PutUint16(buf[12:14], 0)
	binary.BigEndian.PutUint16(buf[14:16], 0)

	_, err := conn.Write(buf)
	return err
}

func readResponse(conn net.Conn) ([]byte, uint16, error) {
	hdr := make([]byte, 16)
	_, err := conn.Read(hdr)
	if err != nil {
		return nil, 0, err
	}

	frameLen := binary.BigEndian.Uint32(hdr[0:4])
	status := binary.BigEndian.Uint16(hdr[14:16])

	payload := make([]byte, frameLen)
	_, err = conn.Read(payload)
	if err != nil {
		return nil, status, err
	}

	return payload, status, nil
}

func appendValue(conn net.Conn, key, value []byte) error {
	payload := make([]byte, 8+len(key)+len(value))

	payload[0] = 2 // strict
	payload[1] = 0

	binary.BigEndian.PutUint16(payload[2:4], uint16(len(key)))
	binary.BigEndian.PutUint32(payload[4:8], uint32(len(value)))

	copy(payload[8:], key)
	copy(payload[8+len(key):], value)

	err := writeHeader(conn, OpcodeAppend, 1, uint32(len(payload)))
	if err != nil {
		return err
	}

	_, err = conn.Write(payload)
	if err != nil {
		return err
	}

	resp, status, err := readResponse(conn)
	if err != nil {
		return err
	}

	fmt.Println("APPEND status:", status)
	fmt.Println("commit_token:", binary.BigEndian.Uint64(resp))

	return nil
}

func getValue(conn net.Conn, key []byte) error {
	payload := make([]byte, 2+len(key))

	binary.BigEndian.PutUint16(payload[0:2], uint16(len(key)))
	copy(payload[2:], key)

	err := writeHeader(conn, OpcodeGet, 2, uint32(len(payload)))
	if err != nil {
		return err
	}

	_, err = conn.Write(payload)
	if err != nil {
		return err
	}

	resp, status, err := readResponse(conn)
	if err != nil {
		return err
	}

	fmt.Println("GET status:", status)

	found := resp[0]
	valueLen := binary.BigEndian.Uint32(resp[4:8])
	value := resp[8 : 8+valueLen]

	fmt.Println("found:", found)
	fmt.Println("value:", string(value))

	return nil
}

func readFrom(conn net.Conn, shard uint16) error {
	payload := make([]byte, 24)

	binary.BigEndian.PutUint16(payload[0:2], shard)
	binary.BigEndian.PutUint16(payload[2:4], 0)
	binary.BigEndian.PutUint32(payload[4:8], 0)
	binary.BigEndian.PutUint64(payload[8:16], 0)
	binary.BigEndian.PutUint32(payload[16:20], 16)
	binary.BigEndian.PutUint32(payload[20:24], 0)

	err := writeHeader(conn, OpcodeReadFrom, 3, uint32(len(payload)))
	if err != nil {
		return err
	}

	_, err = conn.Write(payload)
	if err != nil {
		return err
	}

	resp, status, err := readResponse(conn)
	if err != nil {
		return err
	}

	fmt.Println("READ_FROM status:", status)

	itemCount := binary.BigEndian.Uint32(resp[16:20])
	fmt.Println("item_count:", itemCount)

	off := 24
	for i := 0; i < int(itemCount); i++ {
		seq := binary.BigEndian.Uint64(resp[off : off+8])
		keyLen := binary.BigEndian.Uint16(resp[off+12 : off+14])
		valLen := binary.BigEndian.Uint32(resp[off+16 : off+20])
		off += 20

		key := resp[off : off+int(keyLen)]
		off += int(keyLen)

		val := resp[off : off+int(valLen)]
		off += int(valLen)

		fmt.Printf("item[%d] seq=%d key=%s value=%s\n", i, seq, key, val)
	}

	return nil
}

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:7001")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	appendValue(conn, []byte("user:1"), []byte("Alice"))
	getValue(conn, []byte("user:1"))

	// shard brute force
	for i := 0; i < 4; i++ {
		fmt.Println("---- shard", i)
		readFrom(conn, uint16(i))
	}
}