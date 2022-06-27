package client

import (
	"net"
	"time"

	"github.com/daulet/minikafka"
)

type Subscriber struct {
	addr  string
	conn  *net.TCPConn
	rdr   *minikafka.MessageReader[[]byte]
	topic string
}

type SubscriberConfig func(p *Subscriber)

func SubscriberBrokerAddress(addr string) SubscriberConfig {
	return func(p *Subscriber) {
		p.addr = addr
	}
}

func SubscriberTopic(topic string) SubscriberConfig {
	return func(p *Subscriber) {
		p.topic = topic
	}
}

func NewSubscriber(opts ...SubscriberConfig) (*Subscriber, error) {
	s := &Subscriber{}
	for _, opt := range opts {
		opt(s)
	}
	conn, err := dial("tcp", s.addr, time.Second)
	if conn == nil {
		return nil, err
	}
	s.conn = conn.(*net.TCPConn)
	s.rdr = minikafka.NewMessageReader[[]byte](s.conn)
	// first message is to declare what this subscriber is listening for
	{
		data := []byte(s.topic)
		err = s.rdr.WriteBytes(&data)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Subscriber) Read() ([]byte, error) {
	msg, err := s.rdr.ReadPayload()
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (s *Subscriber) Close() {
	s.conn.Close()
	s.conn = nil
}
