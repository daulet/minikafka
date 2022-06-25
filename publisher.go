package minikafka

import (
	"fmt"
	"net"
	"time"
)

type Publisher struct {
	addr  string
	conn  *MessageReader[[]byte]
	reqs  chan *request
	resps chan chan<- error
	topic string
}

type request struct {
	msg  *[]byte
	resp chan<- error
}

type PublisherConfig func(p *Publisher)

func PublisherBrokerAddress(addr string) PublisherConfig {
	return func(p *Publisher) {
		p.addr = addr
	}
}

func PublisherTopic(topic string) PublisherConfig {
	return func(p *Publisher) {
		p.topic = topic
	}
}

func NewPublisher(opts ...PublisherConfig) (*Publisher, error) {
	p := &Publisher{}
	for _, opt := range opts {
		opt(p)
	}
	if p.topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	conn, err := dial("tcp", p.addr, time.Second)
	if conn == nil {
		return nil, err
	}
	p.conn = NewMessageReader[[]byte](conn.(*net.TCPConn))
	// first message is to declare what this publisher is publishing
	{
		data := []byte(p.topic)
		err = p.conn.WriteBytes(&data)
		if err != nil {
			return nil, err
		}
	}

	p.reqs = make(chan *request, 1000)
	p.resps = make(chan chan<- error, 1000)
	go func() {
		for req := range p.reqs {
			err := p.conn.WriteBytes(req.msg)
			if err != nil {
				req.resp <- fmt.Errorf("error writing message: %v", err)
				continue
			}
			p.resps <- req.resp
		}
	}()
	go func() {
		for resp := range p.resps {
			// wait for ack, there is no way to distinguish between acks for different messages
			buff, err := p.conn.readBytes(2)
			if err != nil {
				resp <- fmt.Errorf("error reading response: %v", err)
				continue
			}
			if string(buff) != "OK" {
				resp <- fmt.Errorf("received a NACK %s", buff)
				continue
			}
			resp <- nil
		}
	}()
	return p, nil
}

func (p *Publisher) Publish(_ string, data []byte) error {
	ch := make(chan error)
	defer close(ch)
	p.reqs <- &request{
		msg:  &data,
		resp: ch,
	}
	return <-ch
}

func (p *Publisher) Close() {
	p.conn.Close()
	p.conn = nil
	close(p.reqs)
	close(p.resps)
}
