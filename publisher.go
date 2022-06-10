package minikafka

import (
	"context"
	"encoding/gob"
	"fmt"
	"net"
	"time"
)

type Publisher struct {
	addr string
	conn *net.TCPConn
}

type PublisherConfig func(p *Publisher)

func PublisherBrokerAddress(addr string) PublisherConfig {
	return func(p *Publisher) {
		p.addr = addr
	}
}

func NewPublisher(opts ...PublisherConfig) (*Publisher, error) {
	p := &Publisher{}
	for _, opt := range opts {
		opt(p)
	}
	conn, err := dial("tcp", p.addr, time.Second)
	if conn == nil {
		return nil, err
	}
	p.conn = conn.(*net.TCPConn)
	return p, nil
}

func (p *Publisher) Publish(ctx context.Context, topic string, data []byte) error {
	encoder := gob.NewEncoder(p.conn)
	err := encoder.Encode(Message{Topic: topic, Payload: data})
	if err != nil {
		return fmt.Errorf("error publishing: %v", err)
	}
	buff := make([]byte, 1024)
	// wait for ack, there is no way to distinguish between acks for different messages
	n, err := p.conn.Read(buff)
	if err != nil {
		return fmt.Errorf("error receiving message: %v", err)
	}
	buff = buff[:n]
	if string(buff) != "OK" {
		return fmt.Errorf("received a NACK %s", buff)
	}
	return nil
}

func (p *Publisher) Close() {
	p.conn.Close()
	p.conn = nil
}
