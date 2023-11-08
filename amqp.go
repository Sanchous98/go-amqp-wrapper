package go_amqp

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/cenkalti/backoff/v4"
	"github.com/gofrs/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type AmqpConnection struct {
	*amqp.Connection

	dsn       string
	reconnect *sync.Cond
	log       Logger

	serializer   marshaller
	unSerializer unMarshaller
}

func NewAmqpConnection(dsn string, log Logger, serializer marshaller, unSerializer unMarshaller) *AmqpConnection {
	if serializer == nil {
		serializer = json.Marshal
	}

	if unSerializer == nil {
		unSerializer = json.Unmarshal
	}

	return &AmqpConnection{
		dsn:          dsn,
		log:          log,
		serializer:   serializer,
		unSerializer: unSerializer,
		reconnect:    sync.NewCond(new(sync.Mutex)),
	}
}

func (a *AmqpConnection) CreateChannel(ctx context.Context, options ...Option) (Channel, error) {
	a.waitForConnect()

	a.reconnect.L.Lock()
	defer a.reconnect.L.Unlock()

	ch, err := a.Connection.Channel()

	if err != nil {
		return nil, err
	}

	o := Build(options)

	if o.Fetch == 0 {
		o.Fetch = 1
	}

	_ = ch.Qos(o.Fetch, 0, false)

	if err = a.assertOptions(ch, o); err != nil {
		panic(err)
	}

	c := &channel{
		tx: make(chan any, o.Fetch),
		rx: make(chan Message, o.Fetch),
	}

	if o.Queue != "" {
		go func() {
			a.consume(ctx, c.rx, ch, o)
			close(c.rx)
		}()
	} else {
		close(c.rx)
	}

	if o.Exchange != "" {
		go func() {
			a.publish(ctx, c.tx, ch, o)
			close(c.tx)
		}()
	} else {
		close(c.tx)
	}

	return c, nil
}

func (a *AmqpConnection) Launch(ctx context.Context) {
	a.connect()

	for {
		// This channel is just for notifying the connection loss
		ch, err := a.Channel()

		if err != nil {
			panic(err)
		}

		select {
		case <-ctx.Done():
			_ = a.Connection.CloseDeadline(time.Now().Add(250 * time.Millisecond))
			return
		case <-ch.NotifyClose(make(chan *amqp.Error)):
			a.log.Infoln("Reconnecting")
			a.connect()
		}
	}
}

func (a *AmqpConnection) connect() {
	a.reconnect.L.Lock()
	defer a.reconnect.Broadcast()
	defer a.reconnect.L.Unlock()

	var err error

	if a.Connection != nil {
		err = a.Connection.CloseDeadline(time.Now().Add(250 * time.Millisecond))
		if !errors.Is(err, amqp.ErrClosed) {
			panic(err)
		}
	}
	a.Connection, err = backoff.RetryWithData(func() (*amqp.Connection, error) {
		a.log.Infoln("Trying to connect")
		return amqp.Dial(a.dsn)
	}, backoff.NewConstantBackOff(5*time.Second))

	if err != nil {
		panic(err)
	}

	a.log.Infoln("Connected")
}

func (a *AmqpConnection) consume(ctx context.Context, rx chan<- Message, ch *amqp.Channel, o Options) {
	messages, err := ch.ConsumeWithContext(ctx, o.Queue, o.RoutingKey, o.AutoAck, o.Exclusive, true, false, nil)

	if err != nil {
		panic(err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case message := <-messages:
			a.log.Debugw("Message received", "queue", o.Queue, "id", message.MessageId, "payload", string(message.Body))
			rx <- amqpMessage{message, a.unSerializer}
		}
	}
}

func (a *AmqpConnection) publish(ctx context.Context, tx <-chan any, ch *amqp.Channel, o Options) {
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-tx:
			messageId := uuid.Must(uuid.NewV4()).String()
			body, err := a.serializer(message)

			if err != nil {
				panic(err)
			}

			err = ch.PublishWithContext(ctx, o.Exchange, o.RoutingKey, true, false, amqp.Publishing{
				Body:      body,
				MessageId: messageId,
			})

			if err != nil {
				a.log.Errorln(err)
			} else {
				a.log.Debugw("Message sent", "exchange", o.Exchange, "id", messageId, "payload", string(body))
			}
		}
	}
}

func (a *AmqpConnection) waitForConnect() {
	a.reconnect.L.Lock()
	if a.Connection == nil || a.Connection.IsClosed() {
		a.reconnect.Wait()
	}
	a.reconnect.L.Unlock()
}

func (a *AmqpConnection) assertOptions(ch *amqp.Channel, o Options) (err error) {
	if o.Exchange != "" {
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
	}

	if o.Queue != "" {
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

		if o.Exchange != "" {
			err = ch.QueueBind(
				q.Name,
				o.RoutingKey,
				o.Exchange,
				false,
				nil,
			)
		}
	}

	return
}
