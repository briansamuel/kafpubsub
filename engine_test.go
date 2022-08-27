package kafka

import (
	"context"
	"fmt"

	"testing"
)

func TestStartKafkaClient(t *testing.T) {

	var brokerURL = "0.0.0.0:9092"

	kafps := NewKafkaPubSub(brokerURL)
	appCtx := NewAppContext(kafps)
	var subcriber Subscriber
	subcriber = NewSubscriber(appCtx)

	subcriber.StartSubTopic(
		"topic-1",
		true,
		Topic1HandleFunction(appCtx),
	)
	subcriber.Start()
	_ = kafps.Publish(context.Background(), "topic-1", nil)
	_ = kafps.Publish(context.Background(), "topic-2", nil)
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

type appCtx struct {
	kafkaPs PubSub
}

func NewAppContext(kafkaPs PubSub) *appCtx {
	return &appCtx{kafkaPs: kafkaPs}
}

func (ctx *appCtx) GetKafka() PubSub { return ctx.kafkaPs }
