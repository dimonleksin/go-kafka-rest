package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/dimonleksin/go-kafka-rest/pkg/kafka/kfk_reader"
	"github.com/dimonleksin/go-kafka-rest/pkg/server/groups"
)

func (s *Server) Instance(write http.ResponseWriter, read *http.Request) {
	if read.Method != "PUT" && read.Method != "DELETE" {
		write.WriteHeader(http.StatusMethodNotAllowed)
		write.Write([]byte("in /groups allowed only DELETE or PUT"))
		return
	}

	g := groups.Group{
		GroupId:  getElementsPath(read.RequestURI)[1],
		Settings: s.Settings,
	}

	if read.Method == "PUT" {
		err := json.NewDecoder(read.Body).Decode(&g)
		if err != nil {
			log.Println("error decode request body", err)
			write.WriteHeader(http.StatusInternalServerError)
			write.Write([]byte("error decode request body"))
			return
		}
		if len(g.Topic) == 0 {
			write.WriteHeader(http.StatusBadRequest)
			write.Write([]byte("topic name not contains in body"))
			return
		}
		if len(s.Stream) == 0 {
			s.Stream = make(map[string]kfk_reader.Kafka)
		}
		serv, err := g.CreateConsumer()
		if err != nil {
			log.Println("error create consumer", err)
			write.WriteHeader(http.StatusInternalServerError)
			write.Write([]byte("error decode request body"))
			return
		}
		s.Stream[g.ClientId] = serv
		write.Write([]byte(fmt.Sprintf("topic: %s, group: %s", g.Topic, g.ClientId)))
	} else if read.Method == "DELETE" {
		if err := s.Stream[g.ClientId].CloseConsumer(); err != nil {
			write.WriteHeader(http.StatusInternalServerError)
			resp := fmt.Sprintf("error closing consumer: %v", err)
			write.Write([]byte(resp))
		}
		write.WriteHeader(http.StatusOK)
		write.Write([]byte("consumer deleted"))
	}
}
