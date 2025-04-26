package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/akrylysov/pogreb"
	"log"
	"net/http"
	"os"
	"time"
	"tinymq/config"
	"tinymq/core/exchange"
	"tinymq/core/storage"
	"tinymq/message"
)

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	storagePath := flag.String("storagePath", "./var", "path to storage")
	dbPath := flag.String("hashtable", "", "file with deduplication database")
	exchangesPath := flag.String("exchanges", "", "json file with exchanges config")
	flag.Parse()

	globalConfig := config.Settings{}
	globalConfig.StoragePath = *storagePath
	if *dbPath != "" {
		db, err := pogreb.Open(*dbPath, nil)
		if err != nil {
			log.Panic(err)
		}

		globalConfig.DB = storage.NewMessageStorage(db)
	} else {
		globalConfig.DB = storage.NewNullMessageStorage()
	}

	globalConfig.StatCollectorInterval = 10 * time.Minute
	globalConfig.GarbageCollectorInterval = 1 * time.Minute

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

	config.InitConfig(globalConfig)

	http.HandleFunc("POST /publish/{exchange}", message.PostPublishHandler)
	http.HandleFunc("GET /consume/{queue}/{count}", message.ConsumeHandler)
	server := fmt.Sprintf(":%d", *port)
	log.Fatal(http.ListenAndServe(server, nil))
}
