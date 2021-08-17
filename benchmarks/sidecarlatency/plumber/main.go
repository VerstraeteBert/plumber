package main

import (
	"io/ioutil"
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// forwards body, which contains a timestamp
		body, err := ioutil.ReadAll(r.Body)
		// body does not need to be close:
		// 	// For server requests, the Request Body is always non-nil
		//	// but will return EOF immediately when no body is present.
		//	// The Server will close the request body. The ServeHTTP
		//	// Handler does not need to.
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(body)
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}
