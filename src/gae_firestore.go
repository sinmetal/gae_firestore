package gae_firestore

import (
	"net/http"
)

func init() {
	http.HandleFunc("/hello", handler)

	m := http.DefaultServeMux
	SetUpItem(m)
}

func handler(w http.ResponseWriter, r *http.Request) {
	// some code
}
