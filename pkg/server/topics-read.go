package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/dimonleksin/go-kafka-rest/pkg/kafka/kfk_reader"
)

func (s *Server) Read(cId string, write http.ResponseWriter, read *http.Request) {
	// slice for saving data from chan
	data := make([]kfk_reader.Message, 1)
	ch := s.Stream[cId].DataChan()
	go s.Stream[cId].ReadDataFromTopic()

	for d := range ch {
		data = append(data, d)
	}
	fmt.Println(data)
	ch = make(chan kfk_reader.Message, 20)
	s.Stream[cId].CreateChan(ch)
	js, err := json.Marshal(data)
	if err != nil {
		write.WriteHeader(http.StatusInternalServerError)
		write.Write([]byte(err.Error()))
		return
	}
	write.Write(js)
}
