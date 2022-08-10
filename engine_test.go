package kafpubsub

import (
	"context"
	"fmt"
	"log"
	"strings"

	"testing"
)

func TestStartKafkaClient(t *testing.T) {

	var brokerURL = "0.0.0.0:9092"
	brokers := strings.Split(brokerURL, ",")
	client := NewClient(brokers)
	kafps := NewKafkaPubSub(client)
	appCtx := NewAppContext(kafps)
	NewSubscriber(appCtx).Start()
	_ = kafps.Publish(context.Background(), "topic-1", nil)
	_ = kafps.Publish(context.Background(), "topic-2", nil)
}

func (sb *subscriber) Start() error {

	sb.Setup()
	return nil
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

	err := sb.InitialClient()
	if err != nil {
		log.Print(err)
	}

}

func Topic1HandleFunction(appCtx AppContext) ConsumerJob {
	return ConsumerJob{
		Title: "Topic1HandleFunction",
		Hdl: func(ctx context.Context, msg *Message) error {

			fmt.Sprintf("Message %s of topic %s", msg.Data(), msg.Chanel())
			return nil
		},
	}
}

func Topic2HandleFunction(appCtx AppContext) ConsumerJob {
	return ConsumerJob{
		Title: "Topic2HandleFunction",
		Hdl: func(ctx context.Context, msg *Message) error {

			fmt.Sprintf("Message %s of topic %s", msg.Data(), msg.Chanel())
			return nil
		},
	}
}

type appCtx struct {
	kafkaPs PubSub
}

func NewAppContext(kafkaPs PubSub) *appCtx {
	return &appCtx{kafkaPs: kafkaPs}
}

func (ctx *appCtx) GetKafka() PubSub { return ctx.kafkaPs }
