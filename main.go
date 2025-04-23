package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/akrylysov/pogreb"
	"log"
	"net/http"
	"os"
	"tinymq/config"
	"tinymq/core"
	"tinymq/core/exchange"
	"tinymq/message"
)

func main() {
	globalConfig := config.Settings{}
	port := flag.Int("port", 8080, "port to listen on")
	globalConfig.StoragePath = flag.String("storagePath", "./var", "path to storage")
	dbPath := flag.String("hashtable", "", "file with deduplication database")
	if *dbPath != "" {
		db, err := pogreb.Open(*dbPath, nil)
		if err != nil {
			log.Panic(err)
		}

		globalConfig.DB = db
	} else {
		globalConfig.DB = new(core.NullStorage)
	}

	exchangesPath := flag.String("exchanges", "", "json file with exchanges config")

	exchangeJson, err := os.ReadFile(*exchangesPath)

	if err != nil {
		log.Panic(err)
	}

	bindings := map[string][]string{}

	err = json.Unmarshal(exchangeJson, &bindings)

	if err != nil {
		log.Panic(err)
	}

	for exchangeName, queues := range bindings {
		for _, queueName := range queues {
			exchange.Bind(exchangeName, queueName)
		}
	}

	http.HandleFunc("POST /publish/{exchange}", message.PostPublishHandler)
	http.HandleFunc("GET /consume/{queue}/{count}", message.ConsumeHandler)
	server := fmt.Sprintf(":%d", *port)
	log.Fatal(http.ListenAndServe(server, nil))
}
