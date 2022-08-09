package kafpubsub

import (
	"context"
	"fmt"
	"strings"

	"log"

	"testing"
)

func TestStartKafkaClient(t *testing.T) {
	brokers := strings.Split("0.0.0.0:9092", ",")
	client := NewClient(brokers)
	appCtx := NewAppContext(client)
	NewSubscriber(appCtx).Start()
}

func (sb *subscriber) Start() {

	err := sb.InitConsumer()
	if err != nil {
		log.Print(err)

	}
	sb.Setup()

}

func (sb *subscriber) Setup() {

	sb.startSubTopic(
		"topic-1",
		true,
		Topic1HandleFunction(sb.appCtx),
	)

	// Follow topic in queue
	sb.startSubTopic(
		"topic-2",
		true,
		Topic2HandleFunction(sb.appCtx),
	)

}

func Topic1HandleFunction(appCtx AppContext) consumerJob {
	return consumerJob{
		Title: "Topic1HandleFunction",
		Hdl: func(ctx context.Context, msg *Message) error {

			fmt.Sprintf("Message %s of topic %s",msg.Data(), msg.Chanel())
			return nil
		},
	}
}

func Topic2HandleFunction(appCtx AppContext) consumerJob {
	return consumerJob{
		Title: "Topic2HandleFunction",
		Hdl: func(ctx context.Context, msg *Message) error {

			fmt.Sprintf("Message %s of topic %s",msg.Data(), msg.Chanel())
			return nil
		},
	}
}

type appCtx struct {
	kafkaPs        PubSub
}

func NewAppContext(kafkaPs PubSub) *appCtx {
	return &appCtx{kafkaPs: kafkaPs}
}

func (ctx *appCtx) GetKafka() PubSub { return ctx.kafkaPs }