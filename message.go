package minikafka

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"time"
)

// TODO if we can avoid gob altogether - just bytes as payload
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
type MessageReader[T any] struct {
	conn   Connection
	buffer []byte
	temp   []byte
}

func (r *MessageReader[_]) SetDeadline(t time.Time) error {
	return r.conn.SetDeadline(t)
}

func (r *MessageReader[T]) Read() (*T, error) {
	b, err := r.readBytes(4)
	if err != nil {
		return nil, err
	}
	n := int(b[0])<<24 | int(b[1])<<16 | int(b[2])<<8 | int(b[3])
	b, err = r.readBytes(n)
	if err != nil {
		return nil, err
	}
	var msg T
	dec := gob.NewDecoder(bytes.NewReader(b))
	err = dec.Decode(&msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

// If your message is just bytes, get just payload, no gob involved
func (r *MessageReader[_]) ReadPayload() ([]byte, error) {
	b, err := r.readBytes(4)
	if err != nil {
		return nil, err
	}
	n := int(b[0])<<24 | int(b[1])<<16 | int(b[2])<<8 | int(b[3])
	b, err = r.readBytes(n)
	if err != nil {
		return nil, err
	}
	cb := make([]byte, len(b))
	copy(cb, b)
	return cb, nil
}

func (r *MessageReader[_]) ReadRaw() ([]byte, error) {
	b, err := r.readBytes(4)
	if err != nil {
		return nil, err
	}
	raw := b
	n := int(b[0])<<24 | int(b[1])<<16 | int(b[2])<<8 | int(b[3])
	b, err = r.readBytes(n)
	if err != nil {
		return nil, err
	}
	raw = append(raw, b...)
	return raw, nil
}

func (r *MessageReader[T]) Write(msg *T) error {
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

func (r *MessageReader[T]) WriteBytes(data *[]byte) error {
	n := len(*data)
	r.conn.Write([]byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)})
	_, err := r.conn.Write(*data)
	return err
}

func (r *MessageReader[_]) Close() error {
	return r.conn.Close()
}

func (r *MessageReader[_]) readBytes(n int) ([]byte, error) {
	if len(r.buffer) < n {
		m, err := io.ReadAtLeast(r.conn, r.temp, n-len(r.buffer))
		if err != nil {
			return nil, err
		}
		r.buffer = append(r.buffer, r.temp[:m]...)
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

func (r *MessageReader[_]) writeBytes(b []byte) (int, error) {
	return r.conn.Write(b)
}

func NewMessageReader[T any](conn Connection) *MessageReader[T] {
	return &MessageReader[T]{
		conn: conn,
		temp: make([]byte, 15*1024), // 15KB is initial TCP packet size
	}
}
