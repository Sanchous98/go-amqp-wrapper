package go_amqp

type Logger interface {
	Infoln(...any)
	Errorln(...any)
	Debugw(string, ...any)
}
