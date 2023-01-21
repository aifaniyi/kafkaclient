package kafkaclient

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type testMsgProc struct {
	id       int
	result   int
	consumer Consumer
	producer Producer
}

func newTestMsgProc(id int, consumer Consumer, producer Producer) *testMsgProc {
	return &testMsgProc{
		id:       id,
		consumer: consumer,
		producer: producer,
	}
}

func (m *testMsgProc) ID() int {
	return m.id
}

func (m *testMsgProc) Execute(ctx context.Context, message *kafka.Message) error {
	// deserialize message
	data := &msg{}
	err := json.Unmarshal(message.Value, data)
	if err != nil {
		return err
	}

	m.result = m.result + data.Value

	// commit offset
	err = m.consumer.CommitMessage(message)
	if err != nil {
		return err
	}

	return nil
}

func (m *testMsgProc) Result() int {
	return m.result
}

type msg struct {
	Value int `json:"value"`
}

func TestConsumer(t *testing.T) {
	ctx := context.Background()

	// setup kafka
	composeFilePaths := []string{"docker-compose.yml"}
	identifier := strings.ToLower("kafkaclient")
	compose := testcontainers.NewLocalDockerCompose(composeFilePaths, identifier)
	defer compose.Down()

	// initialize docker environement
	if err := initialize(compose); err != nil {
		t.Fatalf("error initializing containers: %v", err)
	}

	// setup producer and consumer
	producer, consumer := getClients(t)
	defer producer.Close()
	defer consumer.Close()

	// setup N message processors (workers)
	testMsgProcs := make([]MessageProcessor, 20)

	for i := range testMsgProcs {
		testMsgProcs[i] = newTestMsgProc(i, consumer, producer)
	}

	quit := make(chan struct{})
	defer close(quit)

	// produce a few messages
	produceMessages(t, ctx, producer)

	// send quit signal after timeout
	go func() {
		<-time.After(20 * time.Second)
		quit <- struct{}{}
	}()

	// start consumer
	err := consumer.Consume([]string{"test_topic"}, testMsgProcs, quit)
	if err != nil {
		t.Fatalf("error consuming messages: %v", err)
	}

	result := 0
	for _, r := range testMsgProcs {
		result += r.(*testMsgProc).Result()
	}

	expected := 1000
	if result != expected {
		t.Fatalf("\texpected:\t%d\n\tactual:\t%d", expected, result)
	}
}

func getClients(t *testing.T) (Producer, Consumer) {
	producer, err := NewProducer(&ProducerOpts{
		Brokers: "localhost:29092",
	})
	if err != nil {
		t.Fatalf("error creating producer: %v", err)
		return nil, nil
	}

	// setup consumer
	consumer, err := NewConsumer(&ConsumerOpts{
		Brokers: "localhost:29092",
		Group:   "test_group",
		Offset:  "earliest",
	})
	if err != nil {
		t.Fatalf("error creating consumer: %v", err)
		return nil, nil
	}

	return producer, consumer
}

func initialize(compose *testcontainers.LocalDockerCompose) error {
	compose.Down()

	execError := compose.
		WithCommand([]string{"up", "-d", "kafka"}).
		WaitForService("kafka", wait.NewHostPortStrategy("9092/tcp")).
		Invoke()
	if err := execError.Error; err != nil {
		return err
	}

	return nil
}

func produceMessages(t *testing.T, ctx context.Context, producer Producer) {
	for i := 0; i < 100; i++ {
		value := &msg{
			Value: 10,
		}

		b, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("error serializing message: %v", err)
		}

		if err := producer.Produce(ctx, []byte(fmt.Sprintf("key-%d", i)),
			b, "test_topic", nil); err != nil {
			t.Fatalf("error producing messages to topic: %v", err)
		}
	}
}
