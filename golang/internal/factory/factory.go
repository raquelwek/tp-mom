package factory

import (
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	rmq "github.com/rabbitmq/amqp091-go"
)

const amqpURI = "amqp://guest:guest@%s:%d/"

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {

	conn, err := rmq.Dial(formatURI(connectionSettings))

	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	//                                 durable, autoDelete, exclusive, noWait, , args
	if err != nil {
		return nil, err
	}
	closeErr := make(chan *rmq.Error, 1)
	conn.NotifyClose(closeErr)
	confirms := ch.NotifyPublish(make(chan rmq.Confirmation, 1))
	return &m.QueueMiddleware{QueueName: queueName, Conn: conn, Channel: ch, Confirms: confirms}, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return nil, nil
}

func formatURI(connectionSettings m.ConnSettings) string {
	return fmt.Sprintf(amqpURI, connectionSettings.Hostname, connectionSettings.Port)
}
