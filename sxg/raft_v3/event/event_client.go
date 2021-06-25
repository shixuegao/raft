package event

type ClientData interface {
	ToBytes() []byte
}

type ClientHandler func(applied bool, err error)

func NewClientEvent(data ClientData, handler ClientHandler) *Event {
	return &Event{
		Type:    TypeClient,
		Data:    data,
		Handler: handler,
	}
}
