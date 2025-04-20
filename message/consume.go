package message

import (
	"fmt"
	"log"
	"net/http"
)

func ConsumeHandler(w http.ResponseWriter, r *http.Request) {
	_, err := fmt.Fprintf(w, "P %s", r.URL.Path[0:])
	if err != nil {
		log.Fatal(err)
	}
}
