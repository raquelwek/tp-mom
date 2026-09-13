package factory

import (
	"context"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	rmq "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct { //TODO; Pasar a priv
	ExchangeName string
	QueueName    string
	RoutingKeys  []string
	Connection   *rmq.Connection
	Channel      *rmq.Channel
	CloseErr     chan *rmq.Error
	Confirms     chan rmq.Confirmation
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	return nil
}

func (em *ExchangeMiddleware) StopConsuming() error {
	return nil
}
func (e *ExchangeMiddleware) Send(msg m.Message) error {
	for _, key := range e.RoutingKeys {
		err := e.Channel.PublishWithContext(
			context.Background(),
			e.ExchangeName,
			key,
			false,
			false,
			rmq.Publishing{
				ContentType:  "text/plain",
				Body:         []byte(msg.Body),
				DeliveryMode: rmq.Persistent,
			},
		)
		if err != nil {
			if e.isDisconnected() {
				return m.ErrMessageMiddlewareDisconnected
			}
			return m.ErrMessageMiddlewareMessage
		}

		select {
		case confirm := <-e.Confirms:
			if !confirm.Ack {
				return m.ErrMessageMiddlewareMessage
			}
		default:
			return m.ErrMessageMiddlewareDisconnected
		}
	}
	return nil
}

func (em *ExchangeMiddleware) Close() error {
	return nil
}
func (qm *ExchangeMiddleware) isDisconnected() bool {
	select {
	case <-qm.CloseErr:
		return true
	default:
		return qm.Connection.IsClosed()
	}
}
