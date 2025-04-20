package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"tinymq/config"
	"tinymq/declare"
	"tinymq/message"
)

func main() {
	globalConfig := config.Settings{}
	port := flag.Int("port", 8080, "port to listen on")
	globalConfig.StoragePath = flag.String("storagePath", "./var", "path to storage")

	config.InitConfig(globalConfig)

	http.HandleFunc("GET /exchanges", declare.GetExchangesHandler)
	http.HandleFunc("POST /exchanges", declare.PostExchangesHandler)
	http.HandleFunc("DELETE /exchange/{exchange}", declare.DeleteExchangeHandler)
	http.HandleFunc("GET /queues", declare.GetQueuesHandler)
	http.HandleFunc("POST /queues", declare.PostQueuesHandler)
	http.HandleFunc("DELETE /queue/{queue}", declare.DeleteQueueHandler)
	http.HandleFunc("GET /bind/{exchange}", declare.GetBindHandler)
	http.HandleFunc("POST /bind/{exchange}/{queue}", declare.PostBindHandler)
	http.HandleFunc("DELETE /bind/{exchange}/{queue}", declare.DeleteBindHandler)
	http.HandleFunc("POST /publish/{exchange}", message.PostPublishHandler)
	http.HandleFunc("PUT /publish/{exchange}", message.PutPublishHandler)
	http.HandleFunc("GET /consume/{queue}", message.ConsumeHandler)
	server := fmt.Sprintf(":%d", *port)
	log.Fatal(http.ListenAndServe(server, nil))
}
