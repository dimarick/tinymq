package message

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sync"
	"tinymq/core"
	"tinymq/core/exchange"
	"tinymq/http_core"
)

var exchangeObjectsMutex sync.Mutex
var exchangeObjects = make(map[string]*exchange.ExchangeDescriptor)

func PostPublishHandler(w http.ResponseWriter, r *http.Request) {
	exchangeName := r.PathValue("exchange")

	if !exchange.HasBindings(exchangeName) {
		http_core.ShowResponse(w, http_core.Response{
			Status: http.StatusNotFound,
			Header: http.Header{},
			Body:   "Exchange not found",
		})

		return
	}

	contentType := r.Header.Get("Content-Type")
	var t uint8

	if contentType == "application/json" {
		t = core.TypeJson
	} else if contentType == "application/octet-stream" {
		t = core.TypeBinary
	} else if contentType == "text/plain" {
		t = core.TypeText
	} else {
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte("Content-Type must be set"))

		http_core.ShowError(w, "Content-Type must be application/json", errors.New("invalid type"))

		if err != nil {
			log.Print("Failed to publish", err)
		}

		return
	}

	var messages []core.Message
	err := json.NewDecoder(r.Body).Decode(&messages)
	if err != nil {
		http_core.ShowError(w, "Failed to read request", err)
		return
	}

	for _, msg := range messages {
		msg.ContentType = t
	}

	exchangeObject := getExchange(exchangeName)

	exchangeObject.Publish(&core.Operation{
		Op:       core.OpPublish,
		Target:   exchangeName,
		Messages: messages,
	})

	http_core.ShowResponse(w, http_core.Response{
		Status: http.StatusOK,
		Header: http.Header{},
	})
}

func getExchange(exchangeName string) *exchange.ExchangeDescriptor {
	exchangeObjectsMutex.Lock()
	defer exchangeObjectsMutex.Unlock()
	exchangeObject, ok := exchangeObjects[exchangeName]
	if !ok {
		exchangeObject = exchange.GetExchange(exchangeName)
		exchangeObjects[exchangeName] = exchangeObject
	}
	return exchangeObject
}
