package minikafka_test

import (
	"bytes"
	"encoding/gob"
	"io"
	"testing"
	"time"

	"github.com/daulet/minikafka"
)

// TestConn implements minikafka.Connection
type TestConn struct {
	packets [][]byte
}

func (c *TestConn) SetDeadline(t time.Time) error {
	panic("implement me")
}

func (c *TestConn) Read(b []byte) (int, error) {
	if len(c.packets) == 0 {
		return 0, io.EOF
	}
	n := copy(b, c.packets[0])
	c.packets = c.packets[1:]
	return n, nil
}

func (c *TestConn) Write(b []byte) (int, error) {
	panic("implement me")
}

func (c *TestConn) Close() error {
	panic("implement me")
}

func TestPacketPartitioning(t *testing.T) {
	var (
		packets [][]byte
		size    = 10
	)
	{
		msg := &minikafka.Message{
			Topic:   "test",
			Payload: []byte("hello"),
		}

		var data []byte
		{
			var b bytes.Buffer
			enc := gob.NewEncoder(&b)
			err := enc.Encode(msg)
			if err != nil {
				t.Fatal(err)
			}
			n := b.Len()
			data = append(data, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
			data = append(data, b.Bytes()...)
		}
		for from := 0; from < len(data); from += size {
			to := from + size
			if to > len(data) {
				to = len(data)
			}
			packets = append(packets, data[from:to])
		}
	}
	r := minikafka.NewMessageReader(
		&TestConn{
			packets: packets,
		},
	)
	{
		msg, err := r.Read()
		if err != nil {
			t.Fatal(err)
		}

		if msg.Topic != "test" {
			t.Errorf("expected topic %s, got %s", "test", msg.Topic)
		}
		if !bytes.Equal(msg.Payload, []byte("hello")) {
			t.Errorf("expected payload %s, got %s", "hello", msg.Payload)
		}
	}
}
