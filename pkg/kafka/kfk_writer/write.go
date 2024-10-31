package kfk_writer

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/dimonleksin/go-kafka-rest/pkg/settings"
)

type KafkaWriter struct {
	Data     chan string
	Topic    chan string
	Producer sarama.SyncProducer
	Responce chan Result
	Err      chan error
}

type Result struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

// Write data to topics
func (kfk *KafkaWriter) Write() {
	log.Printf("produccer started")
	for data := range <-kfk.Data {
		msg := sarama.ProducerMessage{
			Topic: <-kfk.Topic,
			Value: sarama.StringEncoder(data),
		}
		log.Printf("send message %v", msg)
		partition, offset, err := kfk.Producer.SendMessage(&msg)
		if err != nil {
			kfk.Err <- err
		}
		r := Result{
			Partition: partition,
			Offset:    offset,
		}
		kfk.Responce <- r
	}
}

// Create produccer instance
func (kfk *KafkaWriter) CreateProducer(s settings.Setting) (err error) {
	cli, err := s.Conf("")
	if err != nil {
		return err
	}
	kfk.Producer, err = sarama.NewSyncProducerFromClient(cli)
	if err != nil {
		return fmt.Errorf("error create produsser. err: %v", err)
	}
	// Start producing in parrent gorutine & wait message
	go kfk.Write()
	return nil
}
