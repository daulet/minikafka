package minikafka

import (
	"bufio"
	"fmt"
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
	var (
		conn    net.Conn
		err     error
		timeout = time.Millisecond
	)
	for timeout < time.Second {
		conn, err = net.DialTimeout("tcp", s.addr, timeout)
		if err != nil {
			fmt.Printf("failed to dial with %v timeout, will retry\n", timeout)
			timeout += timeout
			continue
		}
		break
	}
	if conn == nil {
		return nil, fmt.Errorf("failed to dial after retries up to %v timeout: %v", timeout, err)
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
