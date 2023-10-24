package go_amqp

import (
	"context"
	"encoding/json"
	"github.com/cenkalti/backoff/v4"
	"github.com/gofrs/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type ChannelFactory interface {
	CreateChannel(context.Context, ...Option) (Channel, error)
}

type amqpConnection struct {
	*amqp.Connection

	dsn       string
	reconnect *sync.Cond
	log       Logger

	serializer   marshaller
	unSerializer unMarshaller
}

func NewAmqpConnection(dsn string, log Logger, serializer marshaller, unSerializer unMarshaller) ChannelFactory {
	if serializer == nil {
		serializer = json.Marshal
	}

	if unSerializer == nil {
		unSerializer = json.Unmarshal
	}

	return &amqpConnection{
		dsn:          dsn,
		log:          log,
		serializer:   serializer,
		unSerializer: unSerializer,
		reconnect:    sync.NewCond(new(sync.Mutex)),
	}
}

func (a *amqpConnection) connect() {
	var err error
	a.Connection, err = backoff.RetryWithData(func() (*amqp.Connection, error) {
		a.log.Infoln("Trying to connect")
		return amqp.Dial(a.dsn)
	}, backoff.NewConstantBackOff(5*time.Second))

	if err != nil {
		panic(err)
	}

	a.log.Infoln("Connected")
}

func (a *amqpConnection) CreateChannel(ctx context.Context, options ...Option) (Channel, error) {
	a.reconnect.L.Lock()
	defer a.reconnect.L.Unlock()

	c := &channel{
		tx: make(chan any),
		rx: make(chan Message),
	}

	ch, err := a.Connection.Channel()

	if err != nil {
		return nil, err
	}

	var o Options

	for _, option := range options {
		option.apply(&o)
	}

	err = a.assertOptions(ch, o)

	if err != nil {
		panic(err)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				message, ok, err := ch.Get(o.Queue, o.AutoAck)

				if err != nil {
					a.WaitForConnect()

					ch, err = a.Connection.Channel()

					if err != nil {
						panic(err)
					}

					err = a.assertOptions(ch, o)

					if err != nil {
						panic(err)
					}

					continue
				}

				if !ok {
					continue
				}

				a.log.Debugw("Message received", "queue", o.Queue, "id", message.MessageId, "payload", string(message.Body))
				c.rx <- amqpMessage{message, a.unSerializer}
			}
		}
	}()

	go func() {
		select {
		case <-ctx.Done():
			return
		case message := <-c.tx:
			messageId := uuid.Must(uuid.NewV4()).String()
			body, err := a.serializer(message)

			if err != nil {
				panic(err)
			}

			ch.PublishWithContext(ctx,
				o.Exchange,
				o.RoutingKey,
				true,
				true,
				amqp.Publishing{
					Body:      body,
					MessageId: messageId,
				})
		}
	}()

	return c, nil
}

func (a *amqpConnection) WaitForConnect() {
	a.reconnect.L.Lock()
	if a.Connection == nil || a.Connection.IsClosed() {
		a.reconnect.Wait()
	}
	a.reconnect.L.Unlock()
}

func (a *amqpConnection) Launch(ctx context.Context) {
	a.reconnect.L.Lock()
	a.connect()
	a.reconnect.L.Unlock()
	a.reconnect.Broadcast()

	for {
		// This channel is just for notifying the connection loss
		ch, err := a.Channel()

		if err != nil {
			panic(err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ch.NotifyClose(make(chan *amqp.Error)):
			a.log.Infoln("Reconnecting")
			a.reconnect.L.Lock()
			_ = ch.Close()
			_ = a.Connection.Close()
			a.connect()
			a.reconnect.L.Unlock()
			a.reconnect.Broadcast()
		}
	}
}

func (a *amqpConnection) assertOptions(ch *amqp.Channel, o Options) (err error) {
	err = ch.ExchangeDeclare(
		o.Exchange,
		string(o.Kind),
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return
	}

	var q amqp.Queue
	q, err = ch.QueueDeclare(
		o.Queue,
		true,
		false,
		o.Exclusive,
		false,
		nil,
	)

	if err != nil {
		return
	}

	err = ch.QueueBind(
		q.Name,
		o.RoutingKey,
		o.Exchange,
		false,
		nil,
	)

	return
}
