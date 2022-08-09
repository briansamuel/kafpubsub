package kafpubsub

import (
	"context"
	"github.com/briansamuel/asynjob"
	"log"
)

type User struct {
	Name string
	Age  int
}

type AppContext interface {
	GetKafka() PubSub
}


type consumerJob struct {
	Title string
	Hdl   func(ctx context.Context, msg *Message) error
}

type subscriber struct {
	appCtx AppContext
}

func NewSubscriber(appCtx AppContext) *subscriber {
	return &subscriber{
		appCtx: appCtx,
	}
}

func (sb *subscriber) InitConsumerGroup(group string) error {

	kafka := sb.appCtx.GetKafka()
	err := kafka.InitConsumerGroup(group)
	if err != nil {
		log.Print(err)
		return err
	}

	return nil
}

func (sb *subscriber) InitConsumer() error {

	kafka := sb.appCtx.GetKafka()

	err := kafka.InitConsumer()
	if err != nil {
		log.Print(err)
		return err
	}

	return nil
}

//func (sb *subscriber) Start() error {
//
//	//// Follow topic in queue
//	//kafka := sb.appCtx.GetKafka()
//	//
//	//err := sb.InitConsumerGroup("CG-0")
//	//if err != nil {
//	//	log.Print(err)
//	//
//	//}
//	//
//	//buff := make(chan Message, 5)
//	//go func() {
//	//
//	//	errSub := kafka.Subscribe([]*Topic{{
//	//		Name: common.TopicUserLikeRestaurant, AutoCommit: true}, {
//	//		Name: common.TopicUserDislikeRestaurant, AutoCommit: true}}, 1, buff)
//	//	log.Print(errSub)
//	//}()
//	err := sb.InitConsumer()
//	if err != nil {
//		log.Print(err)
//
//	}
//	sb.Setup()
//
//	return nil
//}

type GroupJob interface {
	Run(ctx context.Context) error
}

func (sb *subscriber) startSubTopic(topic string, isConcurrency bool, consumerJobs ...consumerJob) {

	kafka := sb.appCtx.GetKafka()

	////kafka := sb.appCtx.GetKafka()

	buff := make(chan Message, 5)

	for _, item := range consumerJobs {
		log.Println("Setup Kafka subscriber for:", item.Title)
	}

	getJobHandler := func(job *consumerJob, msg *Message) asyncjob.JobHandler {
		return func(ctx context.Context) error {
			log.Println("running for job", job.Title, ". Value", msg.Data())
			return job.Hdl(ctx, msg)
		}
	}

	go func() {
		for {
			d := <-buff
			log.Println("Kafka Message Dequeue:", d.Topic)
			jobHdlArr := make([]asyncjob.Job, len(consumerJobs))

			for i := range consumerJobs {
				jobHdl := getJobHandler(&consumerJobs[i], &d)

				jobHdlArr[i] = asyncjob.NewJob(jobHdl, asyncjob.WithName(consumerJobs[i].Title))

			}

			groups := asyncjob.NewGroup(isConcurrency, jobHdlArr...)

			if err := groups.Run(context.Background()); err != nil {
				log.Println(err)
			}
		}
	}()

	go func() {
		log.Printf("Kafka Subscribe Topic %s", topic)
		errSub := kafka.OnScanMessages([]string{topic}, buff)
		log.Print(errSub)
	}()

	//go func() {
	//	log.Printf("Kafka Subscribe Topic %s", topic)
	//	errSub := kafka.Subscribe([]*Topic{{
	//		Name: topic, AutoCommit: true}}, 1, buff)
	//	log.Print(errSub)
	//}()

}

