package kafka

import (
	"context"
	asyncjob "github.com/briansamuel/asynjob"

	log "github.com/sirupsen/logrus"
)

type User struct {
	Name string
	Age  int
}

type AppContext interface {
	GetKafka() PubSub
}

type ConsumerJob struct {
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

func (sb *subscriber) GetAppContext() AppContext {

	return sb.appCtx
}

func (sb *subscriber) Start() {

	kafka := sb.appCtx.GetKafka()
	err := kafka.InitialClient(sb.topics...)
	if err != nil {
		log.Print(err)
		return
	}
}

type GroupJob interface {
	Run(ctx context.Context) error
}

func (sb *subscriber) StartSubTopic(topic string, isConcurrency bool, consumerJobs ...ConsumerJob) {

	sb.topics = append(sb.topics, topic)
	c, _ := sb.appCtx.GetKafka().Subscribe(context.Background(), topic)
	////kafka := sb.appCtx.GetKafka()

	getJobHandler := func(job *ConsumerJob, msg *Message) asyncjob.JobHandler {
		return func(ctx context.Context) error {
			log.Trace("running for job", job.Title, ". Value", msg.Data())
			return job.Hdl(ctx, msg)
		}
	}

	go func() {
		for {
			msg := <-c
			log.Trace("Kafka Message Dequeue:", msg.Topic)
			jobHdlArr := make([]asyncjob.Job, len(consumerJobs))

			for i := range consumerJobs {
				jobHdl := getJobHandler(&consumerJobs[i], msg)

				jobHdlArr[i] = asyncjob.NewJob(jobHdl, asyncjob.WithName(consumerJobs[i].Title))

			}

			groups := asyncjob.NewGroup(isConcurrency, jobHdlArr...)

			if err := groups.Run(context.Background()); err != nil {
				log.Error(err)
			}
		}
	}()

}
