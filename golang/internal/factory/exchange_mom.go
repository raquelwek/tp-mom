package factory

import (
	"context"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	rmq "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct { //TODO; Pasar a priv
	exchangeName string
	queueName    string
	routingKeys  []string
	connection   *rmq.Connection
	channel      *rmq.Channel
	closeErr     chan *rmq.Error
	confirms     chan rmq.Confirmation
	consumerTag  string
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	tag := em.queueName + "-consumer"

	deliveries, err := em.channel.Consume(
		em.queueName,
		tag,
		false, // autoAck: manual, porque exponés ack/nack
		false, // exclusive: no hace falta, la cola ya es exclusiva desde QueueDeclare
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		if em.isDisconnected() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	em.consumerTag = tag

	go receiveMessages(deliveries, callbackFunc)
	return nil
}

func (em *ExchangeMiddleware) StopConsuming() error {
	if em.consumerTag == "" {
		return nil // no se estaba consumiendo, no hace nada (como pide la interfaz)
	}

	err := em.channel.Cancel(em.consumerTag, false) // false = noWait
	if err != nil {
		if em.isDisconnected() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage // aunque la interfaz no lo menciona para este método, revisá si aplica
	}

	em.consumerTag = ""
	return nil
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
		case confirm := <-e.confirms:
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
	if em.connection == nil || em.connection.IsClosed() {
		return nil
	}

	if em.consumerTag != "" {
		if err := em.StopConsuming(); err != nil {
			return m.ErrMessageMiddlewareClose
		}
	}

	if em.channel != nil {
		if err := em.channel.Close(); err != nil {
			return m.ErrMessageMiddlewareClose
		}
	}

	if err := em.connection.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}
func (qm *ExchangeMiddleware) isDisconnected() bool {
	select {
	case <-qm.closeErr:
		return true
	default:
		return qm.connection.IsClosed()
	}
}
