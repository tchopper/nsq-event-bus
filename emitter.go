package bus

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	nsq "github.com/nsqio/go-nsq"
)

var ErrTimeoutOccurred error = errors.New("Timeout reached")

// Emitter exposes a interface for emitting and listening for events.
type Emitter interface {
	Emit(topic string, payload interface{}) error
	EmitAsync(topic string, payload interface{}) error
	EmitBulkAsync(topic string, payload []interface{}) error
	Request(topic string, payload interface{}, handler HandlerFunc) error
	EmitAndWaitForResultWithTimeout(topic string, payload interface{}, timeoutDuration time.Duration) (interface{}, error)
}

type eventEmitter struct {
	*nsq.Producer
	address  string
	Lookupds []string
}

// NewEmitter returns a new eventEmitter configured with the
// variables from the config parameter, or returning an non-nil err
// if an error occurred while creating nsq producer.
func NewEmitter(ec EmitterConfig) (emitter Emitter, err error) {
	config := newEmitterConfig(ec)

	address := ec.Address
	if len(address) == 0 {
		address = "localhost:4150"
	}

	producer, err := nsq.NewProducer(address, config)
	if err != nil {
		return
	}

	emitter = &eventEmitter{producer, address, []string{}}

	return
}

// Emit emits a message to a specific topic using nsq producer, returning
// an error if encoding payload fails or if an error occurred while publishing
// the message.
func (ee eventEmitter) Emit(topic string, payload interface{}) (err error) {
	if len(topic) == 0 {
		err = ErrTopicRequired
		return
	}

	body, err := ee.encodeMessage(payload, "")
	if err != nil {
		return
	}

	err = ee.Publish(topic, body)

	return
}

func (ee *eventEmitter) EmitAndWaitForResultWithTimeout(topic string, payload interface{}, timeoutDuration time.Duration) (interface{}, error) {
	var result interface{}
	if err := ee.Request(topic, &payload, func(message *Message) (reply interface{}, err error) {
		result = string(message.Payload)
		message.Finish()
		return
	}); err != nil {
		// handle failure to listen a message
		return nil, err
	}

	timeout := time.NewTimer(timeoutDuration)

	for {
		select {
		case <-timeout.C:
			return nil, ErrTimeoutOccurred
		default:
			if result != nil {
				return result, nil
			}

			<-time.After(50 * time.Millisecond)
		}
	}
}

// Emit emits a message to a specific topic using nsq producer, but does not wait for
// the response from `nsqd`. Returns an error if encoding payload fails and
// logs to console if an error occurred while publishing the message.
func (ee eventEmitter) EmitAsync(topic string, payload interface{}) (err error) {
	if len(topic) == 0 {
		err = ErrTopicRequired
		return
	}

	body, err := ee.encodeMessage(payload, "")
	if err != nil {
		return
	}

	responseChan := make(chan *nsq.ProducerTransaction, 1)

	if err = ee.PublishAsync(topic, body, responseChan, ""); err != nil {
		return
	}

	go func(responseChan chan *nsq.ProducerTransaction) {
		for {
			select {
			case trans := <-responseChan:
				if trans.Error != nil {
					log.Fatalf(trans.Error.Error())
				}
			}
		}
	}(responseChan)

	return
}

// Emit bulk emits multiple messages to a specific topic using nsq producer, but does not wait for
// the response from `nsqd`. Returns an error if encoding payload fails and
// logs to console if an error occurred while publishing the message.
func (ee eventEmitter) EmitBulkAsync(topic string, payloads []interface{}) error {
	var err error
	if len(topic) == 0 {
		err = ErrTopicRequired
		return err
	}
	for _, message := range payloads {
		body, err := ee.encodeMessage(&message, "")
		if err != nil {
			return err
		}

		responseChan := make(chan *nsq.ProducerTransaction, 1)

		if err = ee.PublishAsync(topic, body, responseChan, ""); err != nil {
			return err
		}

		go func(responseChan chan *nsq.ProducerTransaction) {
			for {
				select {
				case trans := <-responseChan:
					if trans.Error != nil {
						log.Fatalf(trans.Error.Error())
					}
				}
			}
		}(responseChan)
	}

	return err
}

// Request a RPC like method which implements request/reply pattern using nsq producer and consumer.
// Returns an non-nil err if an error occurred while creating or listening to the internal
// reply topic or encoding the message payload fails or while publishing the message.
func (ee eventEmitter) Request(topic string, payload interface{}, handler HandlerFunc) (err error) {
	if len(topic) == 0 {
		err = ErrTopicRequired
		return
	}

	if handler == nil {
		err = ErrHandlerRequired
		return
	}

	replyTo, err := ee.genReplyQueue()
	if err != nil {
		return
	}

	if err = ee.createTopic(replyTo); err != nil {
		return
	}

	if err = On(ListenerConfig{
		Topic:       replyTo,
		Channel:     replyTo,
		HandlerFunc: handler,
	}); err != nil {
		return
	}

	body, err := ee.encodeMessage(payload, replyTo)
	if err != nil {
		return
	}

	err = ee.Publish(topic, body)

	return
}

func (ee eventEmitter) encodeMessage(payload interface{}, replyTo string) (body []byte, err error) {
	p, err := json.Marshal(payload)
	if err != nil {
		return
	}

	message := NewMessage(p, replyTo)
	body, err = json.Marshal(message)

	return
}

func (ee eventEmitter) genReplyQueue() (replyTo string, err error) {
	b := make([]byte, 8)
	_, err = rand.Read(b)
	if err != nil {
		return
	}

	hash := hex.EncodeToString(b)
	replyTo = fmt.Sprint(hash, ".ephemeral")

	return
}

func (ee eventEmitter) createTopic(topic string) (err error) {
	s := strings.Split(ee.address, ":")
	port, err := strconv.Atoi(s[1])
	if err != nil {
		return
	}

	uri := "http://" + s[0] + ":" + strconv.Itoa(port+1) + "/topic/create?topic=" + topic
	_, err = http.Post(uri, "application/json; charset=utf-8", nil)

	return
}
