package client

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

const (
	OpcodePing     = 0x0001
	OpcodeAppend   = 0x0002
	OpcodeGet      = 0x0003
	OpcodeReadFrom = 0x0004
	OpcodeStats    = 0x0005
)

type Client struct {
	conn          net.Conn
	nextRequestID uint32
}

type ResponseHeader struct {
	FrameLen  uint32
	Version   uint16
	Opcode    uint16
	RequestID uint32
	Flags     uint16
	Status    uint16
}

type GetResult struct {
	Found bool
	Value []byte
}

type Cursor struct {
	ShardID      uint16
	WalSegmentID uint32
	WalOffset    uint64
}

type ReadItem struct {
	SeqNo      uint64
	Durability uint8
	RecordType uint8
	ValueKind  uint8
	Key        []byte
	Value      []byte
}

type ReadFromResult struct {
	NextCursor Cursor
	Items      []ReadItem
}

func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:          conn,
		nextRequestID: 1,
	}, nil
}

func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) nextID() uint32 {
	id := c.nextRequestID
	c.nextRequestID++
	return id
}

func (c *Client) writeHeader(opcode uint16, requestID uint32, payloadLen uint32) error {
	buf := make([]byte, 16)

	binary.BigEndian.PutUint32(buf[0:4], payloadLen)
	binary.BigEndian.PutUint16(buf[4:6], 1)
	binary.BigEndian.PutUint16(buf[6:8], opcode)
	binary.BigEndian.PutUint32(buf[8:12], requestID)
	binary.BigEndian.PutUint16(buf[12:14], 0)
	binary.BigEndian.PutUint16(buf[14:16], 0)

	_, err := c.conn.Write(buf)
	return err
}

func (c *Client) readResponse() (ResponseHeader, []byte, error) {
	var hdr ResponseHeader

	buf := make([]byte, 16)
	if _, err := io.ReadFull(c.conn, buf); err != nil {
		return hdr, nil, err
	}

	hdr.FrameLen = binary.BigEndian.Uint32(buf[0:4])
	hdr.Version = binary.BigEndian.Uint16(buf[4:6])
	hdr.Opcode = binary.BigEndian.Uint16(buf[6:8])
	hdr.RequestID = binary.BigEndian.Uint32(buf[8:12])
	hdr.Flags = binary.BigEndian.Uint16(buf[12:14])
	hdr.Status = binary.BigEndian.Uint16(buf[14:16])

	payload := make([]byte, hdr.FrameLen)
	if hdr.FrameLen > 0 {
		if _, err := io.ReadFull(c.conn, payload); err != nil {
			return hdr, nil, err
		}
	}

	return hdr, payload, nil
}

func (c *Client) Ping() error {
	requestID := c.nextID()

	if err := c.writeHeader(OpcodePing, requestID, 0); err != nil {
		return err
	}

	hdr, payload, err := c.readResponse()
	if err != nil {
		return err
	}

	if hdr.Status != 0 {
		return fmt.Errorf("ping failed: status=%d", hdr.Status)
	}

	if len(payload) != 8 {
		return fmt.Errorf("ping response payload size invalid: got=%d", len(payload))
	}

	return nil
}

func (c *Client) Append(key, value []byte, durability uint8) error {
	requestID := c.nextID()

	payload := make([]byte, 8+len(key)+len(value))
	payload[0] = durability
	payload[1] = 0
	binary.BigEndian.PutUint16(payload[2:4], uint16(len(key)))
	binary.BigEndian.PutUint32(payload[4:8], uint32(len(value)))
	copy(payload[8:], key)
	copy(payload[8+len(key):], value)

	if err := c.writeHeader(OpcodeAppend, requestID, uint32(len(payload))); err != nil {
		return err
	}

	if _, err := c.conn.Write(payload); err != nil {
		return err
	}

	hdr, payloadResp, err := c.readResponse()
	if err != nil {
		return err
	}

	if hdr.Status != 0 {
		return fmt.Errorf("append failed: status=%d", hdr.Status)
	}

	if len(payloadResp) != 8 {
		return fmt.Errorf("append response payload size invalid: got=%d", len(payloadResp))
	}

	return nil
}

func (c *Client) Get(key []byte) (GetResult, error) {
	requestID := c.nextID()

	payload := make([]byte, 2+len(key))
	binary.BigEndian.PutUint16(payload[0:2], uint16(len(key)))
	copy(payload[2:], key)

	if err := c.writeHeader(OpcodeGet, requestID, uint32(len(payload))); err != nil {
		return GetResult{}, err
	}

	if _, err := c.conn.Write(payload); err != nil {
		return GetResult{}, err
	}

	hdr, payloadResp, err := c.readResponse()
	if err != nil {
		return GetResult{}, err
	}

	if hdr.Status != 0 {
		return GetResult{}, fmt.Errorf("get failed: status=%d", hdr.Status)
	}

	if len(payloadResp) < 8 {
		return GetResult{}, fmt.Errorf("get response payload too short: got=%d", len(payloadResp))
	}

	found := payloadResp[0] == 1
	valueLen := binary.BigEndian.Uint32(payloadResp[4:8])

	if len(payloadResp) < 8+int(valueLen) {
		return GetResult{}, fmt.Errorf("get response value truncated")
	}

	value := make([]byte, valueLen)
	copy(value, payloadResp[8:8+valueLen])

	return GetResult{
		Found: found,
		Value: value,
	}, nil
}

func (c *Client) Stats() (uint32, uint64, error) {
	requestID := c.nextID()

	if err := c.writeHeader(OpcodeStats, requestID, 0); err != nil {
		return 0, 0, err
	}

	hdr, payload, err := c.readResponse()
	if err != nil {
		return 0, 0, err
	}

	if hdr.Status != 0 {
		return 0, 0, fmt.Errorf("stats failed: status=%d", hdr.Status)
	}

	if len(payload) != 12 {
		return 0, 0, fmt.Errorf("invalid stats payload")
	}

	shardCount := binary.BigEndian.Uint32(payload[0:4])
	uptime := binary.BigEndian.Uint64(payload[4:12])

	return shardCount, uptime, nil
}

func (c *Client) ReadFrom(cursor Cursor, limit uint32) (ReadFromResult, error) {
	requestID := c.nextID()

	payload := make([]byte, 24)
	binary.BigEndian.PutUint16(payload[0:2], cursor.ShardID)
	binary.BigEndian.PutUint16(payload[2:4], 0)
	binary.BigEndian.PutUint32(payload[4:8], cursor.WalSegmentID)
	binary.BigEndian.PutUint64(payload[8:16], cursor.WalOffset)
	binary.BigEndian.PutUint32(payload[16:20], limit)
	binary.BigEndian.PutUint32(payload[20:24], 0)

	if err := c.writeHeader(OpcodeReadFrom, requestID, uint32(len(payload))); err != nil {
		return ReadFromResult{}, err
	}

	if _, err := c.conn.Write(payload); err != nil {
		return ReadFromResult{}, err
	}

	hdr, payloadResp, err := c.readResponse()
	if err != nil {
		return ReadFromResult{}, err
	}

	if hdr.Status != 0 {
		return ReadFromResult{}, fmt.Errorf("read_from failed: status=%d", hdr.Status)
	}

	if len(payloadResp) < 24 {
		return ReadFromResult{}, fmt.Errorf("read_from response payload too short: got=%d", len(payloadResp))
	}

	result := ReadFromResult{
		NextCursor: Cursor{
			ShardID:      binary.BigEndian.Uint16(payloadResp[0:2]),
			WalSegmentID: binary.BigEndian.Uint32(payloadResp[4:8]),
			WalOffset:    binary.BigEndian.Uint64(payloadResp[8:16]),
		},
		Items: nil,
	}

	itemCount := binary.BigEndian.Uint32(payloadResp[16:20])
	off := 24

	for i := 0; i < int(itemCount); i++ {
		if len(payloadResp) < off+20 {
			return ReadFromResult{}, fmt.Errorf("read_from item header truncated")
		}

		seqno := binary.BigEndian.Uint64(payloadResp[off : off+8])
		durability := payloadResp[off+8]
		recordType := payloadResp[off+9]
		valueKind := payloadResp[off+10]
		keyLen := binary.BigEndian.Uint16(payloadResp[off+12 : off+14])
		valueLen := binary.BigEndian.Uint32(payloadResp[off+16 : off+20])
		off += 20

		if len(payloadResp) < off+int(keyLen)+int(valueLen) {
			return ReadFromResult{}, fmt.Errorf("read_from item payload truncated")
		}

		key := make([]byte, keyLen)
		copy(key, payloadResp[off:off+int(keyLen)])
		off += int(keyLen)

		value := make([]byte, valueLen)
		copy(value, payloadResp[off:off+int(valueLen)])
		off += int(valueLen)

		result.Items = append(result.Items, ReadItem{
			SeqNo:      seqno,
			Durability: durability,
			RecordType: recordType,
			ValueKind:  valueKind,
			Key:        key,
			Value:      value,
		})
	}

	return result, nil
}