package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

func init() {
	log.SetFlags(log.Lshortfile)
	if kafkaVersion == "" {
		kafkaVersion = "2.5.0"
	}
}

var (
	NUM_PARTITION      = 3
	REPLICATION_FACTOR = 1
	kafkaVersion       = os.Getenv("KAFKA_VERSION")
)

type Client struct {
	brokerURLs    []string
	mProducer     sync.Map
	group         sarama.ConsumerGroup
	consumer      sarama.Consumer // for using consumer mode
	kafkaVersion  sarama.KafkaVersion
	reconnect     chan bool
	consumerGroup string
}

func NewClient(brokerURLs []string) *Client {
	return &Client{brokerURLs: brokerURLs}

}
func makeKafkaConfigPublisher(partitioner sarama.PartitionerConstructor) *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	// config.Producer.Partitioner = sarama.NewManualPartitioner()
	config.Producer.Partitioner = partitioner
	// publisherConfig = config
	return config
}

func newPublisher(topic string, brokerURLs ...string) (sarama.SyncProducer, error) {
	config := makeKafkaConfigPublisher(sarama.NewRandomPartitioner)
	prd, err := sarama.NewSyncProducer(brokerURLs, config)
	if err != nil {
		return nil, err
	}
	return prd, nil
}

// InitPublisher init with addr is url of lookupd
func (ps *Client) InitPublisher(brokerURLs ...string) {
	ps.brokerURLs = brokerURLs
	// ps.producers = make(map[string]sarama.SyncProducer)
}

// Publish sync publish message
func (ps *Client) Publish(topic string, messages ...interface{}) error {
	if strings.Contains(topic, "__consumer_offsets") {
		return errors.New("topic fail")
	}

	if _, ok := ps.mProducer.Load(topic); !ok {
		producer, err := newPublisher(topic, ps.brokerURLs...)
		if err != nil {
			log.Print("[sarama]:", err, "]: topic", topic)
			return err
		}
		ps.mProducer.Store(topic, producer)
	}
	listMsg := make([]*sarama.ProducerMessage, 0)
	for _, msg := range messages {
		bin, err := json.Marshal(msg)
		if err != nil {
			log.Printf("[sarama] error: %v[sarama] msg: %v", err, msg)
		}
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Value:     sarama.StringEncoder(bin),
			Partition: -1,
		}
		// asyncProducer.(sarama.AsyncProducer).Input() <- msg
		listMsg = append(listMsg, msg)
	}
	syncProducer, ok := ps.mProducer.Load(topic)
	if !ok {
		log.Print("not found any sync Producer")
	}
	err := syncProducer.(sarama.SyncProducer).SendMessages(listMsg)
	return err
}

func (ps *Client) createTopic(topic string) error {
	config := sarama.NewConfig()
	config.Version = ps.kafkaVersion
	admin, err := sarama.NewClusterAdmin(ps.brokerURLs, config)
	if err != nil {
		log.Println("[warning]: ", err, ps.brokerURLs)
		return err
	}
	detail := &sarama.TopicDetail{
		NumPartitions:     int32(NUM_PARTITION),
		ReplicationFactor: int16(REPLICATION_FACTOR),
	}
	err = admin.CreateTopic(topic, detail, false)
	if err != nil {
		log.Println("[psub]:", err)
	}
	log.Print(detail)
	return err
}

func (ps *Client) ListTopics(brokers ...string) ([]string, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	cluster, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}
	defer func() {
		cluster.Close()
		config = nil
	}()
	return cluster.Topics()
}

func (ps *Client) OnAsyncSubscribe(topics []*Topic, numberPuller int, buf chan *Message) error {
	if len(topics) == 0 {
		return nil
	}
	var txtTopics []string
	autoCommit := map[string]bool{}
	allTopics, err := ps.ListTopics(ps.brokerURLs...)
	if err != nil {
		log.Print("can't not list topics existed")
		return err
	}
	mTopic := make(map[string]bool)
	for _, topic := range allTopics {
		mTopic[topic] = true
	}
	for _, topic := range topics {
		if strings.Contains(topic.Name, "__consumer_offsets") {
			continue
		}
		if topic.IsNeedManualCreateTopic {
			if _, has := mTopic[topic.Name]; has {
				ps.createTopic(topic.Name)
			}
		}

		txtTopics = append(txtTopics, topic.Name)
		autoCommit[topic.Name] = topic.AutoCommit
		if topic.AutoCommit {
			log.Print("don't forget commit topic: ", topic.Name)
		}
	}

	//for _, topic := range allTopics {
	//	if strings.Contains(topic, "__consumer_offsets") {
	//		continue
	//	}
	//	txtTopics = append(txtTopics, topic)
	//
	//}
	consumer := &ConsumerGroupHandle{
		wg:         &sync.WaitGroup{},
		bufMessage: buf,
		lock:       make(chan bool),
		autoCommit: autoCommit,
		mapChanel:  make(map[string][]chan *Message),
		locker:     new(sync.RWMutex),
	}

	ctx, cancel := context.WithCancel(context.Background())
	consumer.wg.Add(numberPuller)

	for i := 0; i < numberPuller; i++ {
		go func() {
			for {
				// `Consume` should be called inside an infinite loop, when a
				// server-side rebalance happens, the consumer session will need to be
				// recreated to get the new claims
				err := ps.group.Consume(ctx, txtTopics, consumer)
				if err != nil {
					log.Printf("[psub]: %v", err)
					consumer.wg.Done()
					consumer.lock <- true
					break
				}
				consumer.wg = &sync.WaitGroup{}
				consumer.wg.Add(numberPuller)
			}
		}()
	}

	consumer.wg.Wait()
	log.Print("[kafka] start all worker")
	<-consumer.lock
	cancel()
	if err := ps.group.Close(); err != nil {
		log.Printf("Error closing client: %v", err)
		return err
	}
	return nil
}

func newConsumer(brokerURLs ...string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	version, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil {
		log.Printf("Error parsing Kafka version: %v", err)
		return nil, err
	}
	config.Version = version
	// config.Producer.Partitioner =
	// config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	//config.Consumer.Offsets.AutoCommit.Enable = true
	//config.Consumer.Offsets.AutoCommit.Interval = 500 * time.Millisecond
	consumer, err := sarama.NewConsumer(brokerURLs, config)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func (ps *Client) InitConsumerGroup(consumerGroup string) error {
	client, err := newConsumerGroup(consumerGroup, ps.reconnect, ps.brokerURLs...)
	if err != nil {
		return err
	}
	ps.group = client
	//ps.brokerURLs = brokerURLs
	ps.consumerGroup = consumerGroup
	// ps.reconnect = make(chan bool)
	return nil
}
func (ps *Client) InitConsumer(brokerURLs ...string) error {
	client, err := newConsumer(brokerURLs...)
	if err != nil {
		return err
	}
	ps.consumer = client
	ps.brokerURLs = brokerURLs
	return nil
}

func newConsumerGroup(consumerGroup string, reconnect chan bool, brokerURLs ...string) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	// config.ClientID = clientid
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Session.Timeout = 20 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 6 * time.Second
	config.Consumer.MaxProcessingTime = 500 * time.Millisecond
	// config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	version, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil {
		log.Printf("Error parsing Kafka version: %v", err)
		return nil, err
	}
	config.Version = version
	// config.Producer.Partitioner =
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Start with a client
	client, err := sarama.NewClient(brokerURLs, config)
	if err != nil {
		return nil, err
	}

	// Start a new consumer group
	group, err := sarama.NewConsumerGroupFromClient(consumerGroup, client)
	if err != nil {
		client.Close()
		return nil, err
	}
	go func() {
		for err := range group.Errors() {
			log.Println("ERROR:", err)
			sarama.NewConsumerGroupFromClient(consumerGroup, client)
		}
	}()
	return group, nil
}

func (ps *Client) OnScanMessages(topics []string, bufMessage chan Message) error {
	done := make(chan bool)
	for _, topic := range topics {
		if strings.Contains(topic, "__consumer_offsets") {
			continue
		}
		partitions, _ := ps.consumer.Partitions(topic)
		log.Printf("number partitions: %v", len(partitions))

		// this only consumes partition no 1, you would probably want to consume all partitions
		for _, partition := range partitions {
			partitionConsumer, err := ps.consumer.ConsumePartition(topic, partition, 0)
			if nil != err {
				log.Printf("Topic %v Partitions: %v", topic, partition)
				continue
			}
			defer func() {
				if err := partitionConsumer.Close(); err != nil {
					log.Print(err)
				}
			}()
			go func(topic string, partitionConsumer sarama.PartitionConsumer) {
				for {
					select {
					case consumerError := <-partitionConsumer.Errors():
						log.Print(consumerError.Err)
						done <- true
					case msg := <-partitionConsumer.Messages():
						messageHandler(msg, bufMessage)
						partitionConsumer.HighWaterMarkOffset()
					}
				}
			}(topic, partitionConsumer)
		}
	}
	<-done
	return nil
}

func messageHandler(m *sarama.ConsumerMessage, bufMessage chan Message) error {
	if len(m.Value) == 0 {
		// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
		return errors.New("message error")
	}
	msg := Message{
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
	bufMessage <- msg
	return nil
}

func BodyParse(bin []byte, p interface{}) error {
	return json.Unmarshal(bin, p)
}

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
