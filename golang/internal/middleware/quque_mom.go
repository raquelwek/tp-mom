package middleware

import rmq "github.com/rabbitmq/amqp091-go"

type QueueMiddleware struct {
	QueueName string
	Conn      *rmq.Connection
	Channel   *rmq.Channel
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	return nil
}

func (qm *QueueMiddleware) StopConsuming() error {
	return nil
}

func (qm *QueueMiddleware) Send(msg Message) error {
	return nil
}

func (qm *QueueMiddleware) Close() error {
	return nil
}
