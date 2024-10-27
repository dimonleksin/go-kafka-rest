package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/dimonleksin/go-kafka-rest/pkg/kafka"
)

func (s *Server) Topics(write http.ResponseWriter, read *http.Request) {

	fmt.Printf("request path %v", read.RequestURI)
	paths := getElementsPath(read.RequestURI)[1:]
	consumerId := paths[len(paths)-1]
	// slice for saving data from chan
	data := make([]kafka.Message, 1)

	go s.Stream[consumerId].ReadDataFromTopic()

	for d := range s.Stream[consumerId].Data {
		data = append(data, d)
	}
	fmt.Println(data)
	s.Stream[consumerId].Data = make(chan kafka.Message)
	js, err := json.Marshal(data)
	if err != nil {
		write.WriteHeader(http.StatusInternalServerError)
		write.Write([]byte(err.Error()))
		return
	}
	write.Write(js)
}

// Return slice from URI
func getElementsPath(path string) (sl []string) {
	return strings.Split(path, "/")[1:]
}

func readFromTopic() {}
