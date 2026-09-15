package factory

import (
	"context"
	"errors"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"

	"github.com/google/uuid"
	rmq "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	baseMiddleware
}

func (q *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	tag := uuid.NewString()
	return q.startConsuming(tag, callbackFunc)
}

func (qm *QueueMiddleware) Send(msg m.Message) error {
	err := qm.channel.PublishWithContext(
		context.Background(),
		DEFAULT_EXCHANGE, // exchange: vacío = exchange por defecto
		qm.queueName,     // routing key = nombre de la queue
		false,            // mandatory
		false,            // immediate
		rmq.Publishing{
			ContentType:  PLAIN_TEXT_TYPE,
			Body:         []byte(msg.Body),
			DeliveryMode: rmq.Persistent,
		},
	)
	if err != nil {
		if qm.isDisconnected() || errors.Is(err, rmq.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}
	return nil
}
