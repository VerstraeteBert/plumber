package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/benchmark", func(w http.ResponseWriter, r *http.Request) {
		// forwards body, which contains a timestamp
		body, err := ioutil.ReadAll(r.Body)
		// body does not need to be close:
		// 	// For server requests, the Request Body is always non-nil
		//	// but will return EOF immediately when no body is present.
		//	// The Server will close the request body. The ServeHTTP
		//	// Handler does not need to.
		if err != nil {
			log.Fatalln(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, err = http.Post("http://127.0.0.1:3500/v1.0/bindings/dapr-output", "application/json", bytes.NewReader([]byte(fmt.Sprintf("{\"data\": %s, \"operation\": \"create\"}", body))))
		if err != nil {
			log.Fatalln(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(body)

	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}
