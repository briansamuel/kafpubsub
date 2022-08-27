package kafkapb

import (
	"context"
	kafka "github.com/briansamuel/kafpubsub"

	"log"
	"sync"
)

type kafkaPubSub struct {
	messageQueue chan *kafka.Message
	mapChanel    map[string][]chan *kafka.Message
	locker       *sync.RWMutex
	client       *kafka.Client
}

func NewKafkaPubSub(brokerUrl ...string) *kafkaPubSub {
	pb := &kafkaPubSub{
		messageQueue: make(chan *kafka.Message, 10000),
		mapChanel:    make(map[string][]chan *kafka.Message),
		locker:       new(sync.RWMutex),
		client:       kafka.NewClient(brokerUrl),
	}
	pb.run()
	return pb
}

func (ps *kafkaPubSub) InitialClient(topics ...string) error {
	err := ps.client.InitConsumerGroup("CG-0")
	if err != nil {
		log.Print(err)
		return err
	}
	log.Print("init done consumer")
	buff := make(chan *kafka.Message, 1000)
	go func() {
		for {
			d := <-buff
			ps.messageQueue <- d

		}
	}()

	var kafTopics []*kafka.Topic
	for _, topic := range topics {
		kafTopic := &kafka.Topic{Name: topic, AutoCommit: true}
		kafTopics = append(kafTopics, kafTopic)
	}
	go func() {
		err = ps.client.OnAsyncSubscribe(kafTopics, 1, buff)
		log.Print(err)
	}()

	return nil
}

func (ps *kafkaPubSub) Publish(ctx context.Context, topic string, data interface{}) error {

	//go func() {
	//	defer common.Recover()
	//	ps.messageQueue <- data
	//	log.Println("New event published:", data.String(), "with", data.Data())
	//}()
	err := ps.client.Publish(topic, data)
	if err != nil {
		return err
	}
	log.Println("New event published:", topic)

	return nil
}

func (ps *kafkaPubSub) Subscribe(ctx context.Context, topic string) (ch <-chan *kafka.Message, close func()) {
	c := make(chan *kafka.Message)
	ps.locker.Lock()

	if val, ok := ps.mapChanel[topic]; ok {
		val = append(ps.mapChanel[topic], c)
		ps.mapChanel[topic] = val
	} else {
		ps.mapChanel[topic] = []chan *kafka.Message{c}
	}

	ps.locker.Unlock()

	return c, func() {
		log.Println("Unsubscribe: ")

		// Get chans in mapChanel with Topic
		if chans, ok := ps.mapChanel[topic]; ok {
			// Loop chans
			for i := range chans {
				// If chans[i] == c, remove it
				if chans[i] == c {
					chans = append(chans[:i], chans[i+1:]...)

					ps.locker.Lock()
					// Set mapChanel with new chans after remove
					ps.mapChanel[topic] = chans
					ps.locker.Unlock()
					break
				}
			}
		}
	}
}

func (ps *kafkaPubSub) run() error {
	log.Println("PubSub started")

	go func() {
		//defer common.Recover()
		for {
			mess := <-ps.messageQueue // Get message from queue

			if subs, ok := ps.mapChanel[mess.Chanel()]; ok {
				for i := range subs {
					go func(c chan *kafka.Message) {
						//defer common.Recover()
						c <- mess
					}(subs[i])
				}
			}
		}
	}()

	return nil
}
