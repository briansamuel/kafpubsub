package main

import (
	"context"
	"fmt"
	_ "github.com/briansamuel/asynjob"
	"github.com/briansamuel/kafpubsub"
	"log"
	"strings"
	"time"
)

func main() {

	var brokerURL = "0.0.0.0:9092"
	brokers := strings.Split(brokerURL, ",")
	client := kafpubsub.NewClient(brokers)
	kafps := kafpubsub.NewKafkaPubSub(client)
	appCtx := NewAppContext(kafps)
	subscriber := kafpubsub.NewSubscriber(appCtx)
	NewEngine(subscriber).Start()
	kafps.Publish(context.Background(), "topic-4", nil)
	kafps.Publish(context.Background(), "topic-5", nil)

	// Calling Sleep method
	time.Sleep(60 * time.Second)

	// Printed after sleep is over
	fmt.Println("Sleep Over.....")
}

type Engine struct {
	sub kafpubsub.Subscriber
}

func NewEngine(sub kafpubsub.Subscriber) *Engine {
	return &Engine{sub: sub}
}

func (sb *Engine) GetSub() kafpubsub.Subscriber { return sb.sub }

func (sb *Engine) Start() error {

	sb.Setup()
	return nil
}

func (sb *Engine) Setup() {

	sb.GetSub().StartSubTopic(
		"topic-4",
		true,
		Topic1HandleFunction(sb.GetSub().GetAppCtx()),
	)

	// Follow topic in queue
	sb.GetSub().StartSubTopic(
		"topic-5",
		true,
		Topic2HandleFunction(sb.GetSub().GetAppCtx()),
	)

	err := sb.GetSub().InitialClient()
	if err != nil {
		log.Print(err)
	}

}

func Topic1HandleFunction(appCtx kafpubsub.AppContext) kafpubsub.ConsumerJob {
	return kafpubsub.ConsumerJob{
		Title: "Topic1HandleFunction",
		Hdl: func(ctx context.Context, msg *kafpubsub.Message) error {

			fmt.Sprintf("Message %s of topic %s", msg.Data(), msg.Chanel())
			return nil
		},
	}
}

func Topic2HandleFunction(appCtx kafpubsub.AppContext) kafpubsub.ConsumerJob {
	return kafpubsub.ConsumerJob{
		Title: "Topic2HandleFunction",
		Hdl: func(ctx context.Context, msg *kafpubsub.Message) error {

			fmt.Sprintf("Message %s of topic %s", msg.Data(), msg.Chanel())
			return nil
		},
	}
}

type appCtx struct {
	kafkaPs kafpubsub.PubSub
}

func NewAppContext(kafkaPs kafpubsub.PubSub) *appCtx {
	return &appCtx{kafkaPs: kafkaPs}
}

func (ctx *appCtx) GetKafka() kafpubsub.PubSub { return ctx.kafkaPs }
