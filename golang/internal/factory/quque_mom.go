package factory

// amqp091-go
import (
	"context"
	"errors"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"

	"github.com/google/uuid"
	rmq "github.com/rabbitmq/amqp091-go"
)

const DEFAULT_EXCHANGE = ""
const PLAIN_TEXT_TYPE = "text/plain"

type QueueMiddleware struct {
	queueName   string
	consumerTag string
	conn        *rmq.Connection
	channel     *rmq.Channel
	closeErr    chan *rmq.Error
	confirms    chan rmq.Confirmation
}

func (q *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	tag := uuid.NewString()

	deliveries, err := q.channel.Consume(
		q.queueName,
		tag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		if q.isDisconnected() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
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
	err := qm.channel.Cancel(qm.consumerTag, false)

	if err != nil {
		if qm.isDisconnected() || errors.Is(err, rmq.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}

		return m.ErrMessageMiddlewareMessage
	}
	qm.consumerTag = ""
	return nil
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

// mbozunovsky@fi.uba.ar
func (qm *QueueMiddleware) Close() error {
	if qm.conn == nil || qm.conn.IsClosed() {
		return nil
	}

	if qm.consumerTag != "" {
		if err := qm.StopConsuming(); err != nil {
			return m.ErrMessageMiddlewareClose
		}
	}

	if err := qm.conn.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}
func (qm *QueueMiddleware) isDisconnected() bool {
	select {
	case <-qm.closeErr:
		return true
	default:
		return qm.conn.IsClosed()
	}
}

func receiveMessages(deliveries <-chan rmq.Delivery, callbackFunc func(msg m.Message, ack func(), nack func())) {
	for delivery := range deliveries {
		d := delivery
		msg := m.Message{Body: string(d.Body)}
		callbackFunc(msg,
			func() { d.Ack(false) }, func() { d.Nack(false, true) })
	}
}
