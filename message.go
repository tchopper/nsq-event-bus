package bus

import (
	"context"
	"encoding/json"

	nsq "github.com/nsqio/go-nsq"
)

// Message carries nsq.Message fields and methods and
// adds extra fields for handling messages internally.
type Message struct {
	*nsq.Message
	ReplyTo string
	Payload []byte
	Context context.Context `json:"-"`
}

// NewMessage returns a new bus.Message.
func NewMessage(payload []byte, replyTo string) *Message {
	return &Message{
		Payload: payload,
		ReplyTo: replyTo,
		Context: context.Background(),
	}
}

// DecodePayload deserializes data (as []byte) and creates a new struct passed by parameter.
func (m *Message) DecodePayload(v interface{}) (err error) {
	err = json.Unmarshal(m.Payload, v)

	return
}
