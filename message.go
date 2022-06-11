package minikafka

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"time"
)

type Message struct {
	Topic   string
	Payload []byte
}

type Connection interface {
	SetDeadline(t time.Time) error
	Read(p []byte) (n int, err error)
	Write(b []byte) (int, error)
	Close() error
}

// MessageReader wraps a TCP stream to read/write valid Message.
// TODO later make it generic so user can specify the message type.
type MessageReader struct {
	conn   Connection
	buffer []byte
}

func (r *MessageReader) SetDeadline(t time.Time) error {
	return r.conn.SetDeadline(t)
}

func (r *MessageReader) Read() (*Message, error) {
	b, err := r.readBytes(4)
	if err != nil {
		return nil, err
	}
	n := int(b[0])<<24 | int(b[1])<<16 | int(b[2])<<8 | int(b[3])
	b, err = r.readBytes(n)
	if err != nil {
		return nil, err
	}
	var msg Message
	dec := gob.NewDecoder(bytes.NewReader(b))
	err = dec.Decode(&msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (r *MessageReader) Write(msg *Message) error {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(msg)
	if err != nil {
		return err
	}
	n := b.Len()
	r.conn.Write([]byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)})
	_, err = r.conn.Write(b.Bytes())
	return err
}

func (r *MessageReader) Close() error {
	return r.conn.Close()
}

func (r *MessageReader) readBytes(n int) ([]byte, error) {
	if len(r.buffer) < n {
		b := make([]byte, 15*1024) // 15KB is initial TCP packet size
		m, err := io.ReadAtLeast(r.conn, b, n-len(r.buffer))
		if err != nil {
			return nil, err
		}
		r.buffer = append(r.buffer, b[:m]...)
	}

	buff, ptr := make([]byte, n), 0
	for ptr < n && ptr < len(r.buffer) {
		buff[ptr] = r.buffer[ptr]
		ptr++
	}
	r.buffer = r.buffer[ptr:]

	if ptr < n {
		log.Fatalf("readBytes: actual (%v) < expected (%v)", ptr, n)
	}
	return buff, nil
}

func (r *MessageReader) writeBytes(b []byte) (int, error) {
	return r.conn.Write(b)
}

func NewMessageReader(conn Connection) *MessageReader {
	return &MessageReader{
		conn: conn,
	}
}
