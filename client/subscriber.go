package client

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/daulet/minikafka"
)

type Subscriber struct {
	addr  string
	conn  *net.TCPConn
	rdr   *minikafka.MessageReader[[]byte]
	topic string

	wg     sync.WaitGroup
	cancel context.CancelFunc
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
	// send heartbeat to broker every second
	{
		ctx, cancel := context.WithCancel(context.Background())
		s.wg.Add(1)
		go func(ctx context.Context) {
			defer s.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				_, err := s.conn.Write([]byte{0})
				if err != nil {
					return
				}
			}
		}(ctx)
		s.cancel = cancel
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
	s.cancel()
	s.wg.Wait()
	s.conn.Close()
	s.conn = nil
}
