package kafpubsub

import (
	"context"
)

type PubSub interface {
	InitialClient(topics ...string) error
	Publish(ctx context.Context, topic string, data interface{}) error
	Subscribe(ctx context.Context, topic string) (ch <-chan *Message, close func())
	//OnAsyncSubscribe(topics []*Topic, numberPuller int, buf chan *Message) error
	//OnScanMessages(topics []string, bufMessage chan Message) error
	//InitConsumerGroup(consumerGroup string) error
	//InitConsumer(brokerURLs ...string) error
}
