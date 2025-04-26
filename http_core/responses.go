package http_core

import (
	"encoding/json"
	"log"
	"net/http"
)

type Response struct {
	Status int
	Header http.Header
	Body   any
}

func ShowResponse(w http.ResponseWriter, response Response) {
	w.Header().Set("Content-Type", "application/json")

	for key, values := range response.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	responseBody, err := json.Marshal(response.Body)
	if err != nil {
		log.Println("Failed to encode response", err)
	}

	w.WriteHeader(response.Status)
	_, err = w.Write(responseBody)
	if err != nil {
		log.Println("Failed to send response", err)
	}
}
