package minikafka

import (
	"bufio"
	"io"
	"net"
	"time"
)

type Subscriber struct {
	addr string
	conn *net.TCPConn
	scnr *bufio.Scanner
}

type SubscriberConfig func(p *Subscriber)

func SubscriberBrokerAddress(addr string) SubscriberConfig {
	return func(p *Subscriber) {
		p.addr = addr
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
	s.scnr = bufio.NewScanner(conn)
	return s, nil
}

func (s *Subscriber) Read() ([]byte, error) {
	if !s.scnr.Scan() {
		return nil, io.EOF
	}
	return s.scnr.Bytes(), nil
}

func (s *Subscriber) Close() {
	s.conn.Close()
	s.conn = nil
}
