package kfk_reader

type Kafka interface {
	// Consumer sarama.ConsumerGroup,
	ReadDataFromTopic()
	DataChan() chan Message
	CreateChan(chan Message)
	CloseConsumer() error
}
