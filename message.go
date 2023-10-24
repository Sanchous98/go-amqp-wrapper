package go_amqp

import amqp "github.com/rabbitmq/amqp091-go"

type Message interface {
	Id() string
	ParseBody(any) error
	Ack() error
	Reject() error
}

type marshaller = func(any) ([]byte, error)
type unMarshaller = func([]byte, any) error

type amqpMessage struct {
	amqp.Delivery

	unSerializer func(bytes []byte, v any) error
}

func (m amqpMessage) Id() string            { return m.MessageId }
func (m amqpMessage) ParseBody(p any) error { return m.unSerializer(m.Body, &p) }
func (m amqpMessage) Ack() error            { return m.Delivery.Ack(false) }
func (m amqpMessage) Reject() error         { return m.Delivery.Reject(true) }
