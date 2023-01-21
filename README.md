# kafkaclient

A simple wrapper around confluent-kafka-go library. It provides a simple interface for consuming kafka events allowing the developer focus on implementing business logic.

## installation

```bash
go get github.com/aifaniyi/kafkaclient
```

## usage
To use the library, first provide an implementation for the MessageProcessor interface
 
```golang
package main

import (
	"context"
	"log"

	"github.com/aifaniyi/kafkaclient"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// STEP 1: implement MessageProcessor interface
type SampleMessageProcessorImpl struct {
	id       int
	consumer kafkaclient.Consumer
}

func NewSampleMessageProcessorImpl(id int, consumer kafkaclient.Consumer) *SampleMessageProcessorImpl {
	return &SampleMessageProcessorImpl{
		id:       id,
		consumer: consumer,
	}
}

func (s *SampleMessageProcessorImpl) ID() int {
	return s.id
}

func (s *SampleMessageProcessorImpl) Execute(ctx context.Context, message *kafka.Message) error {
	// business logic
    log.Println(string(message.Value))

	s.consumer.CommitMessage(message)
	return nil
}

func main() {
	// create consumer
	consumer, err := kafkaclient.NewConsumer(&kafkaclient.ConsumerOpts{
		Brokers: "localhost:29092",
		Group:   "test_group",
		Offset:  "earliest",
	})
	if err != nil {
		log.Fatalf("error creating consumer: %v", err)
	}
	defer consumer.Close()

	// create N message processor (worker) instances
	msgProcs := make([]kafkaclient.MessageProcessor, 20)

	for i := range msgProcs {
		msgProcs[i] = NewSampleMessageProcessorImpl(i, consumer)
	}

	quit := make(chan struct{})
	defer close(quit)

	// start consuming messages (blocking operation)
	err = consumer.Consume([]string{"test_topic"}, msgProcs, quit)
	if err != nil {
		log.Fatalf("error consuming messages: %v", err)
	}
}
```