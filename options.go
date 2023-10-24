package go_amqp

const (
	Direct  Kind = "direct"
	Fanout  Kind = "fanout"
	Headers Kind = "headers"
	Topic   Kind = "topic"
)

const (
	TxOnlyDirection Direction = 1
	RxOnlyDirection Direction = 2
	BothDirections  Direction = TxOnlyDirection | RxOnlyDirection
)

type Direction byte
type Kind string

type Options struct {
	Queue      string
	Exchange   string
	RoutingKey string
	Kind       Kind
	Exclusive  bool
	AutoAck    bool
	Direction  Direction
}

type Option interface {
	apply(*Options)
}

type OptionFunc func(*Options)

func (o OptionFunc) apply(op *Options) {
	o(op)
}

func RouteTo(routingKey string) Option {
	return OptionFunc(func(options *Options) {
		options.RoutingKey = routingKey
	})
}

func ExchangeKind(kind Kind) Option {
	switch kind {
	case Direct, Fanout, Headers, Topic:
	default:
		panic("Invalid exchange kind")
	}

	return OptionFunc(func(options *Options) {
		options.Kind = kind
	})
}

func BindToExchange(exchange string) Option {
	return OptionFunc(func(options *Options) {
		options.Exchange = exchange
	})
}

func Queue(queue string) Option {
	return OptionFunc(func(options *Options) {
		options.Queue = queue
	})
}

func ChannelDirection(direction Direction) Option {
	return OptionFunc(func(options *Options) {
		options.Direction = direction
	})
}

func WithAutoAck(ack bool) Option {
	return OptionFunc(func(options *Options) {
		options.AutoAck = ack
	})
}

func ExclusiveQueue(exclusive bool) Option {
	return OptionFunc(func(options *Options) {
		options.Exclusive = exclusive
	})
}
