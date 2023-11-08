package go_amqp

const (
	Direct  Kind = "direct"
	Fanout  Kind = "fanout"
	Headers Kind = "headers"
	Topic   Kind = "topic"
)

type Kind string

type Options struct {
	Queue      string
	Exchange   string
	RoutingKey string
	Kind       Kind
	Fetch      int
	Exclusive  bool
	AutoAck    bool
}

func Build(options []Option) (o Options) {
	for _, option := range options {
		option.apply(&o)
	}
	return
}

type Option interface {
	apply(*Options)
}

type OptionFunc func(*Options)

func (o OptionFunc) apply(op *Options) { o(op) }

func Fetch(count uint) Option {
	return OptionFunc(func(options *Options) {
		options.Fetch = int(count)
	})
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
