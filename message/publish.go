package message

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"tinymq/core"
	"tinymq/http_core"
)

func PostPublishHandler(w http.ResponseWriter, r *http.Request) {
	_, err := fmt.Fprintf(w, "P %s", r.URL.Path[0:])
	if err != nil {
		log.Fatal(err)
	}
}

func PutPublishHandler(w http.ResponseWriter, r *http.Request) {
	//exchange := r.PathValue("exchange")
	contentType := r.Header.Get("Content-Type")

	if contentType != "application/json" {
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte("Content-Type must be application/json"))

		http_core.ShowError(w, "Content-Type must be application/json", errors.New("Invalid type"))

		if err != nil {
			log.Print("Failed to publish", err)
		}

		return
	}

	messages := []core.Message{}
	err := json.NewDecoder(r.Body).Decode(&messages)
	if err != nil {
		http_core.ShowError(w, "Failed to read request", err)
		return
	}

	//op_log.RegisterOperation(core.Operation{
	//	Op:       core.OpPublishUnique,
	//	Target:   exchange,
	//	Messages: messages,
	//})

	http_core.ShowResponse(w, http_core.Response{
		Status: http.StatusOK,
		Header: http.Header{},
		Body:   "Published messages",
	})
}
