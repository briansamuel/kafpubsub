package kafka

import (
	"errors"
	"github.com/Shopify/sarama"
	"log"
	"sync"
)

// ConsumerGroupHandle represents a Sarama consumer group consumer
type ConsumerGroupHandle struct {
	wg         *sync.WaitGroup
	lock       chan bool
	bufMessage chan *Message
	mapChanel  map[string][]chan *Message
	locker     *sync.RWMutex
	autoCommit map[string]bool
}

func (consumer *ConsumerGroupHandle) Setup(ss sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	log.Print("done: ", ss.MemberID())
	consumer.wg.Done()
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *ConsumerGroupHandle) Cleanup(ss sarama.ConsumerGroupSession) error {
	log.Print("sarama clearuppp: ", ss.MemberID())
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *ConsumerGroupHandle) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for m := range claim.Messages() {
		// log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s, partion = %v", string(message.Value), message.Timestamp, message.Topic, message.Partition)
		// messageHandler(message, consumer.bufMessage, session)
		if len(m.Value) == 0 {
			// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
			return errors.New("message error")
		}
		msg := &Message{
			Topic:     m.Topic,
			Body:      m.Value,
			Offset:    m.Offset,
			Partition: int(m.Partition),
			Timestamp: m.Timestamp.Unix(),
		}
		if len(m.Headers) != 0 {
			headers := map[string]string{}
			for _, header := range m.Headers {
				headers[string(header.Key)] = string(header.Value)
			}
			msg.Headers = headers
		}
		if consumer.autoCommit[m.Topic] {
			session.MarkOffset(m.Topic, m.Partition, m.Offset, "")
			session.MarkMessage(m, "")
			consumer.bufMessage <- msg
			msg.Commit = func() {}
			continue
		}
		msg.Commit = func() {
			session.MarkOffset(m.Topic, m.Partition, m.Offset, "")
			session.MarkMessage(m, "")
		}
		consumer.bufMessage <- msg
		log.Println("Message dequeue", msg.String())

		if subs, ok := consumer.mapChanel[msg.Chanel()]; ok {
			for i := range subs {
				go func(c chan *Message) {
					//defer common.Recover()
					c <- msg
				}(subs[i])
			}
		}
	}
	return nil
}

func (ps *Client) Close() error {
	if ps.consumer != nil {
		if err := ps.consumer.Close(); err != nil {
			return err
		}
	}
	// var err error
	// ps.mProducer.Range(func(k interface{}, sp interface{}) bool {
	// 	if sp == nil {
	// 		return true
	// 	}
	// 	err = sp.(sarama.SyncProducer).Close()
	// 	if err != nil {
	// 		log.Print("close error: ", err.Error())
	// 	}
	// 	return true
	// })
	return nil
}
