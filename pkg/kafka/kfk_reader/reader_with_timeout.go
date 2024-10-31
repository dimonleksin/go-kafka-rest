package kfk_reader

import (
	"context"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type KafkaWithTimeout struct {
	Topic    string        // Name of topic
	Timeout  time.Duration //Timeout in miliseconds from client request
	Consumer sarama.ConsumerGroup
	Counter  int          // Number of message to read
	Data     chan Message // chan for send msg to main gorutine. When chanel close, we need send responce to client
}

func (kfk *KafkaWithTimeout) DataChan() chan Message {
	return kfk.Data
}

func (kfk *KafkaWithTimeout) CreateChan(chan Message) {
	kfk.Data = make(chan Message, 20)
}

func (kfk *KafkaWithTimeout) CloseConsumer() error {
	err := kfk.Consumer.Close()
	return err
}

func (kfk *KafkaWithTimeout) ReadDataFromTopic() {
	var topics []string
	topics = append(topics, kfk.Topic)
	consumer := ConsumerWithTimeout{
		kfk:   kfk,
		ready: make(chan bool),
	}
	consumer.ready = make(chan bool)
	log.Println("consumer ready")
	log.Printf("start reading from %v", topics)
	// Create context for stoping read
	ctx, _ := context.WithTimeout(context.Background(), kfk.Timeout)
	err := kfk.Consumer.Consume(ctx, topics, &consumer)
	if err != nil {
		log.Printf("err: %v", err)
	}
}

// Consumer represents a Sarama consumer group consumer
type ConsumerWithTimeout struct {
	ready chan bool
	kfk   *KafkaWithTimeout
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer ConsumerWithTimeout) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer ConsumerWithTimeout) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (consumer ConsumerWithTimeout) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
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
			// log.Printf("Message claimed: value = %s", string(message.Value))
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
