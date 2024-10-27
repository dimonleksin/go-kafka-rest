package server

import (
	"github.com/dimonleksin/go-kafka-rest/pkg/kafka"
	"github.com/dimonleksin/go-kafka-rest/pkg/settings"
)

type Server struct {
	Stream   map[string]*kafka.Kafka
	Settings settings.Setting
}
