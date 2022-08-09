# KafkaPubsub
> move by briansamuel/kafpubsub helper use concurrent to managerment job test.

[![Go Reference](https://pkg.go.dev/badge/github.com/princjef/gomarkdoc.svg)](https://pkg.go.dev/github.com/briansamuel/kafpubsub)

### Install

``` bash
 go get github.com/briansamuel/kafpubsub
```

### Usage

Library have 3 modules publisher, consumer, engine subscriber.
For example to set up:

* Create appContext Struct

``` go
type appCtx struct {
	kafkaPs        PubSub
}

func NewAppContext(kafkaPs PubSub) *appCtx {
	return &appCtx{kafkaPs: kafkaPs}
}

func (ctx *appCtx) GetKafka() PubSub { return ctx.kafkaPs }
```


* Create Handle for each Topic Subscriber


``` go

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
```


* Start subscriber engine
``` go

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
```

* Test Init Kafka Client and Subscriber Start Example
``` go

func TestStartKafkaClient(t *testing.T) {
	brokers := strings.Split("0.0.0.0:9092", ",")
	client := NewClient(brokers)
	appCtx := NewAppContext(client)
	NewSubscriber(appCtx).Start()
	
	
}
```

* Publish Message With Topic
``` go

func TestPublishMessage(t *testing.T) {
	Publish(common.TopicUserDislikeRestaurant, data)
}
```