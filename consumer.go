package kafkaclient

import (
	"context"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type MessageProcessor interface {
	// returns identifier for message processor
	ID() int
	// implements business logic.
	// Handles commiting of offsets. It typically receives a copy
	// of the consumer interface as a parameter
	Execute(ctx context.Context, message *kafka.Message) error
}

type Consumer interface {
	Consume(topics []string, process []MessageProcessor, close chan struct{}) error
	CommitMessage(m *kafka.Message) error
	Close()
}

type ConsumerOpts struct {
	// comma separated list of kafka brokers
	//   example: host1:9092,host2:9092
	Brokers string
	// consumer group name
	Group string
	// topic location from which events can be consumed
	//   example: earliest, latest
	Offset string
}

type ConsumerImpl struct {
	consumer *kafka.Consumer
}

func NewConsumer(options *ConsumerOpts) (*ConsumerImpl, error) {

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               options.Brokers,
		"group.id":                        options.Group,
		"auto.offset.reset":               options.Offset,
		"go.application.rebalance.enable": true, // delegate Assign() responsibility to app
		"session.timeout.ms":              6000,
		"broker.address.family":           "v4",
	})
	if err != nil {
		return nil, err
	}

	return &ConsumerImpl{
		consumer: consumer,
	}, nil
}

func (c *ConsumerImpl) Consume(topics []string, messageProcessors []MessageProcessor,
	quit chan struct{}) error {

	if len(messageProcessors) < 1 {
		return fmt.Errorf("at least one message processor implementation must be provided")
	}

	if err := c.consumer.SubscribeTopics(topics, nil); err != nil {
		return err
	}

	events := make(chan *kafka.Message, len(messageProcessors))
	defer close(events)

	for _, messmessageProcessor := range messageProcessors {
		go process(messmessageProcessor, events)
	}

	for {
		select {
		case <-quit:
			return nil

		default:
			ev := c.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				events <- e

			case kafka.Error:
				log.Printf("error_code: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					return fmt.Errorf(e.Error())
				}

			default:
				log.Printf("ignored %v\n", e)
			}

		}
	}
}

func (c *ConsumerImpl) CommitMessage(message *kafka.Message) error {
	_, err := c.consumer.CommitMessage(message)
	if err != nil {
		return err
	}

	return nil
}

func (c *ConsumerImpl) Close() {
	c.consumer.Close()
}

func process(messageProcessor MessageProcessor, events <-chan *kafka.Message) {
	for event := range events {
		log.Printf("worker %d received message %v", messageProcessor.ID(), event)

		if err := messageProcessor.Execute(context.TODO(), event); err != nil {
			log.Printf("message processing error: %v", err.Error())
		}
	}
}
