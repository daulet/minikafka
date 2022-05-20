package minikafka

import (
	"context"
	"fmt"

	"github.com/zeromq/goczmq"
)

type Publisher struct {
	addr   string
	dealer *goczmq.Sock
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

	dealer, err := goczmq.NewDealer(p.addr)
	if err != nil {
		return nil, err
	}
	p.dealer = dealer

	return p, nil
}

func (p *Publisher) Publish(ctx context.Context, topic string, data []byte) error {
	err := p.dealer.SendFrame(data, goczmq.FlagNone)
	if err != nil {
		return fmt.Errorf("error sending frame: %v", err)
	}
	// wait for ack, there is no way to distinguish between acks for different messages
	reply, err := p.dealer.RecvMessage()
	if err != nil {
		return fmt.Errorf("error receiving message: %v", err)
	}
	if string(reply[0]) != "OK" {
		return fmt.Errorf("received a NACK %s", reply[0])
	}
	return nil
}

func (p *Publisher) Close() {
	p.dealer.Destroy()
	p.dealer = nil
}
