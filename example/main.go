package main

import (
	"context"
	"fmt"
	_ "github.com/briansamuel/asynjob"
	kafka "github.com/briansamuel/kafpubsub"
	"time"
)

func main() {

	var brokerURL = "0.0.0.0:9092"

	kafps := kafka.NewKafkaPubSub(brokerURL)
	appCtx := NewAppContext(kafps)
	var subcriber kafka.Subscriber
	subcriber = kafka.NewSubscriber(appCtx)

	subcriber.StartSubTopic(
		"topic-1",
		true,
		Topic1HandleFunction(appCtx),
	)
	subcriber.Start()
	_ = kafps.Publish(context.Background(), "topic-1", nil)
	_ = kafps.Publish(context.Background(), "topic-2", nil)

	// Calling Sleep method
	time.Sleep(60 * time.Second)

	// Printed after sleep is over
	fmt.Println("Sleep Over.....")
}

func Topic1HandleFunction(appCtx *appCtx) kafka.ConsumerJob {
	return kafka.ConsumerJob{
		Title: "Topic1HandleFunction",
		Hdl: func(ctx context.Context, msg *kafka.Message) error {

			fmt.Sprintf("Message %s of topic %s", msg.Data(), msg.Chanel())
			return nil
		},
	}
}

type appCtx struct {
	kafkaPs kafka.PubSub
}

func NewAppContext(kafkaPs kafka.PubSub) *appCtx {
	return &appCtx{kafkaPs: kafkaPs}
}

func (ctx *appCtx) GetKafka() kafka.PubSub { return ctx.kafkaPs }
