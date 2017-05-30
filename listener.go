package bus

import (
	"encoding/json"
	"errors"
	"time"

	nsq "github.com/nsqio/go-nsq"
)

var (
	// ErrTopicRequired is returned when topic is not passed as parameter.
	ErrTopicRequired = errors.New("topic is mandatory")
	// ErrHandlerRequired is returned when handler is not passed as parameter.
	ErrHandlerRequired = errors.New("handler is mandatory")
	// ErrChannelRequired is returned when channel is not passed as parameter in bus.ListenerConfig.
	ErrChannelRequired = errors.New("channel is mandatory")
)

type HandlerFunc func(m *Message) (interface{}, error)

type EventListener struct {
	*nsq.Consumer
	listenerConfig ListenerConfig
	worker         HandlerFunc
	middleware     []Middleware
}

// On listen to a message from a specific topic using nsq consumer, returns
// an error if topic and channel not passed or if an error occurred while creating
// nsq consumer.
func On(lc ListenerConfig) (err error) {
	if len(lc.Topic) == 0 {
		err = ErrTopicRequired
		return
	}

	if len(lc.Channel) == 0 {
		err = ErrChannelRequired
		return
	}

	if lc.HandlerFunc == nil {
		err = ErrHandlerRequired
		return
	}

	if len(lc.Lookup) == 0 {
		lc.Lookup = []string{"localhost:4161"}
	}

	if lc.HandlerConcurrency == 0 {
		lc.HandlerConcurrency = 1
	}

	config := newListenerConfig(lc)
	consumer, err := nsq.NewConsumer(lc.Topic, lc.Channel, config)
	if err != nil {
		return
	}

	handler := handleMessage(lc)
	consumer.AddConcurrentHandlers(handler, lc.HandlerConcurrency)
	err = consumer.ConnectToNSQLookupds(lc.Lookup)

	return
}

func NewConsumer(topic, channel string, worker HandlerFunc) *EventListener {
	el := EventListener{}
	el.listenerConfig = ListenerConfig{
		Topic:   topic,
		Channel: channel,
	}

	el.worker = worker

	return &el
}

func (el *EventListener) Start(environment string) (*nsq.Consumer, error) {

	if len(el.listenerConfig.Topic) == 0 {
		err := ErrTopicRequired
		return nil, err
	}

	if len(el.listenerConfig.Channel) == 0 {
		err := ErrChannelRequired
		return nil, err
	}

	if el.worker == nil {
		err := ErrHandlerRequired
		return nil, err
	}

	if len(el.listenerConfig.Lookup) == 0 {
		el.listenerConfig.Lookup = []string{"localhost:4161"}
	}

	if el.listenerConfig.HandlerConcurrency == 0 {
		el.listenerConfig.HandlerConcurrency = 1
	}

	config := newListenerConfig(el.listenerConfig)
	consumer, err := nsq.NewConsumer(el.listenerConfig.Topic, el.listenerConfig.Channel, config)

	if environment == "production" {
		consumer.SetLogger(&nopLogger{}, 0)
	}

	if err != nil {
		return nil, err
	}

	handler := wrapHandle(el)
	consumer.AddConcurrentHandlers(handler, el.listenerConfig.HandlerConcurrency)
	err = consumer.ConnectToNSQLookupds(el.listenerConfig.Lookup)

	return consumer, err
}

func (el *EventListener) SetConcurrency(concurrency int) {
	el.listenerConfig.HandlerConcurrency = concurrency
}

func (el *EventListener) SetMaxInFlight(maxInFlight int) {
	el.listenerConfig.MaxInFlight = maxInFlight
}

func (el *EventListener) SetMaxAttempts(attempts uint16) {
	el.listenerConfig.MaxAttempts = attempts
}

func wrapHandle(el *EventListener) nsq.HandlerFunc {
	return nsq.HandlerFunc(func(message *nsq.Message) (err error) {
		m := Message{Message: message}
		if err = json.Unmarshal(message.Body, &m); err != nil {
			return
		}
		res, err := chain(el.middleware, el.worker)(&m)
		if err != nil {
			return
		}

		if m.ReplyTo == "" {
			return
		}

		emitter, err := NewEmitter(EmitterConfig{LookupdPollInterval: 10 * time.Millisecond})
		if err != nil {
			return
		}
		println("EMIT")
		err = emitter.Emit(m.ReplyTo, res)
		println("EMIT DONE")

		return
	})
}

//deprecated for middleware approach - Emiter still uses this
func handleMessage(lc ListenerConfig) nsq.HandlerFunc {
	return nsq.HandlerFunc(func(message *nsq.Message) (err error) {
		m := Message{Message: message}
		if err = json.Unmarshal(message.Body, &m); err != nil {
			return
		}

		res, err := lc.HandlerFunc(&m)
		if err != nil {
			return
		}

		if m.ReplyTo == "" {
			return
		}

		emitter, err := NewEmitter(EmitterConfig{})
		if err != nil {
			return
		}

		err = emitter.Emit(m.ReplyTo, res)

		return
	})
}
