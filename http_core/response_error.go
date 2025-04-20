package http_core

import (
	"encoding/json"
	"log"
	"net/http"
)

type ResponseError struct {
	Error string
}

func ShowError(w http.ResponseWriter, errorText string, cause error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)

	buffer, err := json.Marshal(ResponseError{
		Error: errorText,
	})

	if err != nil {
		log.Print("Failed to serialize", err, errorText, cause)
	}

	_, err = w.Write(buffer)

	if err != nil {
		log.Print("Failed to publish", err, errorText, cause)
	}

	log.Print("Response error", errorText, cause)
}
