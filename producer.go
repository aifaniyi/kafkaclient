package kafkaclient

import (
	"context"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer interface {
	Produce(ctx context.Context, key, value []byte, topic string, headers map[string]string) error
	Close()
}

type ProducerOpts struct {
	Brokers string
}

type ProducerImpl struct {
	producer *kafka.Producer
}

func NewProducer(options *ProducerOpts) (*ProducerImpl, error) {

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  options.Brokers,
		"enable.idempotence": true,
		"batch.size":         16384,
		"linger.ms":          5,
	})
	if err != nil {
		return nil, err
	}

	// handle delivery errors
	go func(producer *kafka.Producer) {
		for ev := range producer.Events() {
			switch msg := ev.(type) {
			case *kafka.Message:
				if err = msg.TopicPartition.Error; err != nil {
					log.Printf("error publishing message to kafka: %v; error: %v\n", msg.TopicPartition, err)
				}
			}
		}
	}(producer)

	return &ProducerImpl{
		producer: producer,
	}, nil
}

func (p *ProducerImpl) Produce(ctx context.Context, key, value []byte, topic string, headers map[string]string) error {
	hds := []kafka.Header{}

	for k, v := range headers {
		hds = append(hds, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}

	if err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
		Headers:        hds,
	}, nil); err != nil {
		return err
	}

	return nil
}

func (p *ProducerImpl) Close() {
	p.producer.Close()
}
