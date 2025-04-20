package declare

import (
	"fmt"
	"log"
	"net/http"
)

func GetBindHandler(w http.ResponseWriter, r *http.Request) {
	_, err := fmt.Fprintf(w, "G %s", r.URL.Path[0:])
	if err != nil {
		log.Fatal(err)
	}
}

func PostBindHandler(w http.ResponseWriter, r *http.Request) {
	_, err := fmt.Fprintf(w, "P %s", r.URL.Path[0:])
	if err != nil {
		log.Fatal(err)
	}
}

func DeleteBindHandler(w http.ResponseWriter, r *http.Request) {
	_, err := fmt.Fprintf(w, "D %s", r.URL.Path[0:])
	if err != nil {
		log.Fatal(err)
	}
}
