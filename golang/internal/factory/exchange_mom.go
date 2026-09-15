package factory

import (
	"context"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	rmq "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct { //TODO; Pasar a priv
	baseMiddleware
	exchangeName string
	routingKeys  []string
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	tag := em.queueName + "-consumer"
	return em.startConsuming(tag, callbackFunc)
}

func (e *ExchangeMiddleware) Send(msg m.Message) error {
	for _, key := range e.routingKeys {
		err := e.channel.PublishWithContext(
			context.Background(),
			e.exchangeName,
			key,
			false,
			false,
			rmq.Publishing{
				ContentType:  PLAIN_TEXT_TYPE,
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
		case confirm := <-e.confirms:
			if !confirm.Ack {
				return m.ErrMessageMiddlewareMessage
			}
		case <-e.closeErr:
			return m.ErrMessageMiddlewareDisconnected
		}
	}
	return nil
}
