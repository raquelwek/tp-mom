package middleware

// amqp091-go
import (
	"context"
	"errors"

	"github.com/google/uuid"
	rmq "github.com/rabbitmq/amqp091-go"
)

const DEFAULT_EXCHANGE = ""
const PLAIN_TEXT_TYPE = "text/plain"

type QueueMiddleware struct {
	QueueName   string
	consumerTag string
	Conn        *rmq.Connection
	Channel     *rmq.Channel
	closeErr    chan *rmq.Error
	Confirms    chan rmq.Confirmation
}

func (q *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	tag := uuid.NewString()

	deliveries, err := q.Channel.Consume(
		q.QueueName,
		tag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		if q.isDisconnected() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}
	q.consumerTag = tag
	go receiveMessages(deliveries, callbackFunc)
	return nil
}

func (qm *QueueMiddleware) StopConsuming() error {
	if qm.consumerTag == "" {
		// aun no estaba conectado, no tiene efecto
		return nil
	}
	err := qm.Channel.Cancel(qm.consumerTag, false)

	if err != nil {
		if qm.isDisconnected() || errors.Is(err, rmq.ErrClosed) {
			return ErrMessageMiddlewareDisconnected
		}

		return ErrMessageMiddlewareMessage
	}
	qm.consumerTag = ""
	return nil
}

func (qm *QueueMiddleware) Send(msg Message) error {
	err := qm.Channel.PublishWithContext(
		context.Background(),
		DEFAULT_EXCHANGE, // exchange: vacío = exchange por defecto
		qm.QueueName,     // routing key = nombre de la queue
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
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}
	return nil
}

// mbozunovsky@fi.uba.ar
func (qm *QueueMiddleware) Close() error {
	if qm.Conn == nil || qm.Conn.IsClosed() {
		return nil
	}

	if qm.consumerTag != "" {
		if err := qm.StopConsuming(); err != nil {
			return ErrMessageMiddlewareClose
		}
	}

	if err := qm.Conn.Close(); err != nil {
		return ErrMessageMiddlewareClose
	}

	return nil
}
func (qm *QueueMiddleware) isDisconnected() bool {
	select {
	case <-qm.closeErr:
		return true
	default:
		return qm.Conn.IsClosed()
	}
}

func receiveMessages(deliveries <-chan rmq.Delivery, callbackFunc func(msg Message, ack func(), nack func())) {
	for delivery := range deliveries {
		d := delivery
		msg := Message{Body: string(d.Body)}
		callbackFunc(msg,
			func() { d.Ack(false) }, func() { d.Nack(false, true) })
	}
}
