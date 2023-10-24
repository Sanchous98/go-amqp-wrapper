package go_amqp

type Logger interface {
	Infoln(...any)
	Debugw(string, ...any)
}
