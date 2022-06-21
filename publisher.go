package minikafka

import (
	"fmt"
	"net"
	"time"
)

type Publisher struct {
	addr string
	conn *MessageReader
	reqs chan *request
}

type request struct {
	msg  *Message
	resp chan<- error
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
	p.reqs = make(chan *request, 1000)
	// TODO decouple reading from writing so we can publish as fast as we can
	go func() {
		for req := range p.reqs {
			err := p.conn.Write(req.msg)
			if err != nil {
				req.resp <- fmt.Errorf("error writing message: %v", err)
				continue
			}
			// wait for ack, there is no way to distinguish between acks for different messages
			buff, err := p.conn.readBytes(2)
			if err != nil {
				req.resp <- fmt.Errorf("error reading response: %v", err)
				continue
			}
			if string(buff) != "OK" {
				req.resp <- fmt.Errorf("received a NACK %s", buff)
				continue
			}
			req.resp <- nil
		}
	}()
	return p, nil
}

func (p *Publisher) Publish(topic string, data []byte) error {
	ch := make(chan error)
	defer close(ch)
	p.reqs <- &request{
		msg:  &Message{Topic: topic, Payload: data},
		resp: ch,
	}
	return <-ch
}

func (p *Publisher) Close() {
	p.conn.Close()
	p.conn = nil
	close(p.reqs)
}
