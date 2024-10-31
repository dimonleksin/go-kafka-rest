package server

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/dimonleksin/go-kafka-rest/pkg/kafka/kfk_reader"
	"github.com/dimonleksin/go-kafka-rest/pkg/kafka/kfk_writer"
	"github.com/dimonleksin/go-kafka-rest/pkg/settings"
)

type Server struct {
	Stream    map[string]kfk_reader.Kafka
	Produccer *kfk_writer.KafkaWriter
	Settings  settings.Setting
}

// Return slice from URI
func getElementsPath(path string) []string {
	return strings.Split(path, "/")[1:]
}

func (s Server) Topics(write http.ResponseWriter, read *http.Request) {
	fmt.Printf("request path %v", read.RequestURI)
	paths := getElementsPath(read.RequestURI)[1:]
	d := paths[len(paths)-1]
	if read.Method == http.MethodGet {
		s.Read(d, write, read)
	} else if read.Method == http.MethodPost {
		s.WriteToTopic(d, "", write, read)
	} else {
		write.WriteHeader(http.StatusMethodNotAllowed)
	}
}
