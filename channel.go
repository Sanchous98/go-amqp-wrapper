package go_amqp

type Tx interface {
	Tx() chan<- any
}

type Rx interface {
	Rx() <-chan Message
}

type Channel interface {
	Tx
	Rx
}

type channel struct {
	tx chan any
	rx chan Message
}

func (c *channel) Tx() chan<- any     { return c.tx }
func (c *channel) Rx() <-chan Message { return c.rx }
