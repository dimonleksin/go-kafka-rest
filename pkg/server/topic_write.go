package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/dimonleksin/go-kafka-rest/pkg/kafka/kfk_writer"
)

type Result struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

func (s *Server) WriteToTopic(topic string, data string, write http.ResponseWriter, read *http.Request) {
	log.Printf("send message")
	s.Produccer.Topic <- topic
	s.Produccer.Data <- data
	select {
	case err := <-s.Produccer.Err:
		write.WriteHeader(http.StatusInternalServerError)
		write.Write(
			[]byte(fmt.Sprintf("error write to topic: %v", err)),
		)
		return
	case r := <-s.Produccer.Responce:
		jr, err := json.Marshal(r)
		if err != nil {
			write.WriteHeader(http.StatusInternalServerError)
			write.Write(
				[]byte(fmt.Sprintf("error encodinc responce: %v", err)),
			)
		}
		write.WriteHeader(http.StatusOK)
		write.Write(
			jr,
		)
	}
}

func (s *Server) StartProduccer() {
	s.Produccer = &kfk_writer.KafkaWriter{
		Data:     make(chan string),
		Topic:    make(chan string),
		Responce: make(chan kfk_writer.Result),
		Err:      make(chan error),
	}
	err := s.Produccer.CreateProducer(s.Settings)
	if err != nil {
		panic(
			fmt.Errorf("error starting producer: %v", err),
		)
	}
}
