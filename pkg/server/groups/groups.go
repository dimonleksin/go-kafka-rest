package groups

import (
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/dimonleksin/go-kafka-rest/pkg/kafka/kfk_reader"
	"github.com/dimonleksin/go-kafka-rest/pkg/settings"
)

type Group struct {
	GroupId  string
	ClientId string
	Topic    string `json:"topic"`
	Counter  int    `json:"counter"` // Number of message to read
	Timeout  int    `json:"timeout"`
	Settings settings.Setting
}

func (g *Group) CreateConsumer() (kfk kfk_reader.Kafka, err error) {
	var (
		consumer sarama.ConsumerGroup
	)

	cli, err := g.Settings.Conf(g.GroupId)
	if err != nil {
		return nil, err
	}
	g.ClientId = g.RandConsumerId(30)
	chn := make(chan kfk_reader.Message, 50)
	consumer, err = sarama.NewConsumerGroupFromClient(g.GroupId, cli)

	if err != nil {
		return nil, err
	}
	if g.Counter > 0 {
		// ctx, _ := context.WithTimeout(context.Background(), g.Settings.Timeout)
		// defer consumer.Close()
		kfk = &kfk_reader.KafkaWithCounter{
			Topic:    g.Topic,
			Data:     chn,
			Consumer: consumer,
			Counter:  g.Counter,
			Timeout:  g.Settings.Timeout,
			// Ctx:      ctx,
		}
	} else if g.Counter == 0 {
		if g.Timeout == 0 {
			g.Timeout = int(g.Settings.Timeout)
		}
		timeout := time.Duration(g.Timeout) * time.Millisecond
		kfk = &kfk_reader.KafkaWithTimeout{
			Topic:    g.Topic,
			Data:     chn,
			Consumer: consumer,
			Counter:  g.Counter,
			Timeout:  timeout,
		}
	}
	log.Printf("successfuly create group")
	return kfk, nil
}
