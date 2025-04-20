package declare

import (
	"fmt"
	"log"
	"net/http"
)

func GetQueuesHandler(w http.ResponseWriter, r *http.Request) {
	_, err := fmt.Fprintf(w, "G %s", r.URL.Path[0:])
	if err != nil {
		log.Fatal(err)
	}
}

func PostQueuesHandler(w http.ResponseWriter, r *http.Request) {
	_, err := fmt.Fprintf(w, "P %s", r.URL.Path[0:])
	if err != nil {
		log.Fatal(err)
	}
}

func DeleteQueueHandler(w http.ResponseWriter, r *http.Request) {
	_, err := fmt.Fprintf(w, "D %s", r.URL.Path[0:])
	if err != nil {
		log.Fatal(err)
	}
}
