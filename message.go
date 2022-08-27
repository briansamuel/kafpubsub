package kafka

import (
	"fmt"
	"time"
)

// SenderConfig addion config when publish message
type SenderConfig struct {
	Metadata interface{}
	Headers  map[string]string
}

type Topic struct {
	Name                    string
	AutoCommit              bool
	Partition               *int32
	IsNeedManualCreateTopic bool
}

// Message define message encode/decode sarama message
type Message struct {
	Id            string
	Offset        int64  `json:"offset,omitempty"`
	Partition     int    `json:"partition,omitempty"`
	Topic         string `json:"topic,omitempty"`
	Body          []byte `json:"body,omitempty"`
	Timestamp     int64  `json:"timestamp,omitempty"`
	ConsumerGroup string `json:"consumer_group,omitempty"`
	Commit        func()
	Headers       map[string]string
}

func NewMessage(data interface{}) *Message {

	return &Message{
		Id:        fmt.Sprintf("%d", time.Now().UnixNano()),
		Timestamp: time.Now().UnixNano(),
	}
}

func (evt *Message) String() string {
	return fmt.Sprintf("Message %s", evt.Topic)
}

func (evt *Message) Chanel() string {
	return evt.Topic
}

func (evt *Message) SetChanel(chanel string) {
	evt.Topic = chanel
}

func (evt *Message) Data() interface{} {
	return evt.Body
}

func ToInt32(in *int32) int32 {
	return *in
}

func ToPInt32(in int32) *int32 {
	return &in
}
