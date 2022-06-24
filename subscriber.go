package minikafka

import (
	"net"
	"time"
)

type Subscriber struct {
	addr  string
	conn  *net.TCPConn
	rdr   *MessageReader
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
	s.rdr = NewMessageReader(s.conn)
	// first message is to declare what this subscriber is listening for
	s.rdr.Write(&Message{
		Topic: s.topic,
	})
	return s, nil
}

func (s *Subscriber) Read() ([]byte, error) {
	msg, err := s.rdr.Read()
	if err != nil {
		return nil, err
	}
	return msg.Payload, nil
}

func (s *Subscriber) Close() {
	s.conn.Close()
	s.conn = nil
}
