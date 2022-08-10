package kafpubsub

import (
	"context"
	"github.com/briansamuel/asynjob"
	"log"
)

type AppContext interface {
	GetKafka() PubSub
}

type consumerJob struct {
	Title string
	Hdl   func(ctx context.Context, msg *Message) error
}

type subscriber struct {
	appCtx AppContext
	topics []string
}

func NewSubscriber(appCtx AppContext) *subscriber {
	return &subscriber{
		appCtx: appCtx,
	}
}

func (sb *subscriber) InitialClient() error {

	kafka := sb.appCtx.GetKafka()
	err := kafka.InitialClient(sb.topics...)
	if err != nil {
		log.Print(err)
		return err
	}

	return nil
}

type GroupJob interface {
	Run(ctx context.Context) error
}

func (sb *subscriber) startSubTopic(topic string, isConcurrency bool, consumerJobs ...consumerJob) {

	sb.topics = append(sb.topics, topic)
	c, _ := sb.appCtx.GetKafka().Subscribe(context.Background(), topic)
	////kafka := sb.appCtx.GetKafka()

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
			msg := <-c
			log.Println("Kafka Message Dequeue:", msg.Topic)
			jobHdlArr := make([]asyncjob.Job, len(consumerJobs))

			for i := range consumerJobs {
				jobHdl := getJobHandler(&consumerJobs[i], msg)

				jobHdlArr[i] = asyncjob.NewJob(jobHdl, asyncjob.WithName(consumerJobs[i].Title))

			}

			groups := asyncjob.NewGroup(isConcurrency, jobHdlArr...)

			if err := groups.Run(context.Background()); err != nil {
				log.Println(err)
			}
		}
	}()

}
