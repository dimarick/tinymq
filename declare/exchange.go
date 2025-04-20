package declare

import (
	"fmt"
	"log"
	"net/http"
)

func GetExchangeHandler(w http.ResponseWriter, r *http.Request) {
	_, err := fmt.Fprintf(w, "G %s", r.URL.Path[0:])
	if err != nil {
		log.Fatal(err)
	}
}

func GetExchangesHandler(w http.ResponseWriter, r *http.Request) {
	_, err := fmt.Fprintf(w, "G %s", r.URL.Path[0:])
	if err != nil {
		log.Fatal(err)
	}
}

func PostExchangesHandler(w http.ResponseWriter, r *http.Request) {
	_, err := fmt.Fprintf(w, "P %s", r.URL.Path[0:])
	if err != nil {
		log.Fatal(err)
	}
}

func DeleteExchangeHandler(w http.ResponseWriter, r *http.Request) {
	_, err := fmt.Fprintf(w, "D %s", r.URL.Path[0:])
	if err != nil {
		log.Fatal(err)
	}
}
