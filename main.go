package main

import (
	"log"
	"net/http"

	"github.com/dimonleksin/go-kafka-rest/pkg/server"
	"github.com/dimonleksin/go-kafka-rest/pkg/settings"
)

func main() {
	s := settings.Setting{}
	s.GetSettings()
	err := s.VerifyConf()
	if err != nil {
		log.Panicf("failed to start server, err: %v", err)
	}
	server := &server.Server{
		Settings: s,
	}
	http.HandleFunc("/topics/", server.Topics)
	http.HandleFunc("/groups/", server.Instance)
	log.Println("server started in :8080")
	http.ListenAndServe(":8000", nil)
}
