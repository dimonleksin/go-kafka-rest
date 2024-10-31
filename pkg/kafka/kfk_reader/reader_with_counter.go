package kfk_reader

import (
	"context"
	"log"
	"time"

	"github.com/IBM/sarama"
)

// struct for create consumer with counter
type KafkaWithCounter struct {
	Topic    string // Name of topic
	Timeout  time.Duration
	Consumer sarama.ConsumerGroup
	Counter  int          // Number of message to read
	Data     chan Message // chan for send msg to main gorutine. When chanel close, we need send responce to client
	// Ctx      context.Context
}

type Message struct {
	Value  string `json:"value"`
	Offset int    `json:"offset"`
	Finaly bool   `json:"finaly"`
}

func (kfk *KafkaWithCounter) DataChan() chan Message {
	return kfk.Data
}

func (kfk *KafkaWithCounter) CreateChan(chan Message) {
	kfk.Data = make(chan Message, 20)
}

func (kfk *KafkaWithCounter) CloseConsumer() error {
	err := kfk.Consumer.Close()
	return err
}

func (kfk *KafkaWithCounter) ReadDataFromTopic() {
	var topics []string
	topics = append(topics, kfk.Topic)
	consumer := Consumer{
		kfk:   kfk,
		ready: make(chan bool),
	}
	ctx, _ := context.WithTimeout(context.Background(), kfk.Timeout)
	log.Printf("start reading from %v", topics)
	err := kfk.Consumer.Consume(ctx, topics, &consumer)
	if err != nil {
		log.Printf("err: %v", err)
	}
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
	kfk   *KafkaWithCounter
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer Consumer) Setup(sarama.ConsumerGroupSession) error {
	log.Println("creating consumer")
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (consumer Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("start consuming with counter %d", consumer.kfk.Counter)
	for i := 0; i < consumer.kfk.Counter; i++ {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}
			log.Printf("Message claimed: value = %s", string(message.Value))
			consumer.kfk.Data <- Message{
				Value:  string(message.Value),
				Offset: int(message.Offset),
			}
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			log.Println("timeout consuming")
			consumer.kfk.Data <- Message{Finaly: true}
			close(consumer.kfk.Data)
			return nil
		}
	}
	// Close chan for stop waiting and send responce to client
	close(consumer.kfk.Data)
	return nil
}
