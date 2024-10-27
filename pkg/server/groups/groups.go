package groups

import (
	"log"

	"github.com/IBM/sarama"
	"github.com/dimonleksin/go-kafka-rest/pkg/kafka"
	"github.com/dimonleksin/go-kafka-rest/pkg/settings"
)

type Group struct {
	GroupId  string
	ClientId string
	Topic    string `json:"topic"`
	Counter  int    `json:"counter"` // Number of message to read
	Settings settings.Setting
}

func (g *Group) CreateConsumer() (kfk *kafka.Kafka, err error) {
	var (
		consumer sarama.ConsumerGroup
	)

	cli, err := g.Settings.Conf(g.GroupId)
	if err != nil {
		return nil, err
	}
	g.ClientId = g.RandConsumerId(30)
	chn := make(chan kafka.Message, 50)
	consumer, err = sarama.NewConsumerGroupFromClient(g.GroupId, cli)

	if err != nil {
		return nil, err
	}

	// defer consumer.Close()
	kfk = &kafka.Kafka{
		Topic:    g.Topic,
		Data:     chn,
		Consumer: consumer,
		Counter:  g.Counter,
		Timeout:  g.Settings.Timeout,
	}
	log.Printf("successfuly create group with addr %v", kfk.Counter)
	return kfk, nil
}
