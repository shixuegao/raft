package event

import "sxg/raft/v3/util"

const (
	TypeCommand = iota
	TypeRemote
	TypeClient
)

type Event struct {
	Type    byte
	Data    interface{}
	Handler interface{}
}

func NewRemoteEvent(data, handler interface{}) *Event {
	return &Event{
		Type:    TypeRemote,
		Data:    data,
		Handler: handler,
	}
}

type EventQueue struct {
	eventChan chan *Event
}

func (eq *EventQueue) Close() {
	close(eq.eventChan)
}

func (eq *EventQueue) Len() int {
	return len(eq.eventChan)
}

func (eq *EventQueue) Cap() int {
	return cap(eq.eventChan)
}

func (eq *EventQueue) RawChan() chan *Event {
	return eq.eventChan
}

func (eq *EventQueue) TryGet() *Event {
	select {
	case e := <-eq.eventChan:
		return e
	default:
		return nil
	}
}

func (eq *EventQueue) GetSync() (e *Event, ok bool) {
	e, ok = <-eq.eventChan
	return
}

func (eq *EventQueue) GetAsync(f func(e *Event, ok bool)) {
	go func() {
		e, ok := eq.GetSync()
		f(e, ok)
	}()
}

func (eq *EventQueue) TryPut(e *Event) (ok bool) {
	select {
	case eq.eventChan <- e:
		return true
	default:
		return false
	}
}

func (eq *EventQueue) PutSync(e *Event) (ok bool) {
	defer func() {
		if err := util.AssertRecoverErr(); err == nil {
			ok = true
		}
	}()
	eq.eventChan <- e
	return
}

func (eq *EventQueue) PutAsync(e *Event, f func(bool)) {
	go func() {
		f(eq.PutSync(e))
	}()
}

func NewEventQueue(size int) *EventQueue {
	eq := EventQueue{
		eventChan: make(chan *Event, size),
	}
	return &eq
}
