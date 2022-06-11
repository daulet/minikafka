package minikafka

import (
	"context"
	"fmt"
	"net"
	"time"
)

type Publisher struct {
	addr string
	conn *MessageReader
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
	p.conn = &MessageReader{conn: conn.(*net.TCPConn)}
	return p, nil
}

func (p *Publisher) Publish(ctx context.Context, topic string, data []byte) error {
	msg := &Message{Topic: topic, Payload: data}
	err := p.conn.Write(msg)
	if err != nil {
		return fmt.Errorf("error writing message: %v", err)
	}
	// wait for ack, there is no way to distinguish between acks for different messages
	buff, err := p.conn.readBytes(2)
	if err != nil {
		return fmt.Errorf("error reading response: %v", err)
	}
	if string(buff) != "OK" {
		return fmt.Errorf("received a NACK %s", buff)
	}
	return nil
}

func (p *Publisher) Close() {
	p.conn.Close()
	p.conn = nil
}
