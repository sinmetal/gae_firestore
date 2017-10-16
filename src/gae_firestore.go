package gae_firestore

import (
	"net/http"

	"bigtable"
)

func init() {
	http.HandleFunc("/hello", handler)

	m := http.DefaultServeMux
	SetUpItem(m)
	SetUpItemFire(m)
	bigtable.SetUpItemBigtable(m)
	bigtable.SetUpBigtable(m)
}

func handler(w http.ResponseWriter, r *http.Request) {
	// some code
}
