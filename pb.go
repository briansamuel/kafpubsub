package kafpubsub

import (
	"context"
	"log"
	"sync"
)

type kafkaPubSub struct {
	messageQueue chan *Message
	mapChanel    map[string][]chan *Message
	locker       *sync.RWMutex
	client       *Client
}

func NewKafkaPubSub(client *Client) *kafkaPubSub {
	pb := &kafkaPubSub{
		messageQueue: make(chan *Message, 10000),
		mapChanel:    make(map[string][]chan *Message),
		locker:       new(sync.RWMutex),
		client:       client,
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
	buff := make(chan *Message, 10000)
	go func() {
		for {
			d := <-buff
			ps.messageQueue <- d

		}
	}()
	var kafTopics []*Topic
	for _, topic := range topics {
		kaftopic := &Topic{Name: topic, AutoCommit: true}
		kafTopics = append(kafTopics, kaftopic)
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

func (ps *kafkaPubSub) Subscribe(ctx context.Context, topic string) (ch <-chan *Message, close func()) {
	c := make(chan *Message)
	ps.locker.Lock()

	if val, ok := ps.mapChanel[topic]; ok {
		val = append(ps.mapChanel[topic], c)
		ps.mapChanel[topic] = val
	} else {
		ps.mapChanel[topic] = []chan *Message{c}
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
					go func(c chan *Message) {
						//defer common.Recover()
						c <- mess
					}(subs[i])
				}
			}
		}
	}()

	return nil
}
