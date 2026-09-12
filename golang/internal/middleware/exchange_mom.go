package middleware

import rmq "github.com/rabbitmq/amqp091-go"

type ExchangeMiddleware struct {
	ExchangeName string
	QueueName    string
	RoutingKeys  []string
	Connection   *rmq.Connection
	Channel      *rmq.Channel
	CloseErr     chan *rmq.Error
	Confirms     chan rmq.Confirmation
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	return nil
}

func (em *ExchangeMiddleware) StopConsuming() error {
	return nil
}
func (em *ExchangeMiddleware) Send(msg Message) error {
	return nil
}

func (em *ExchangeMiddleware) Close() error {
	return nil
}
