package gae_firestore

import (
	"net/http"

	"bigeg"
	"bigtable"
)

func init() {
	http.HandleFunc("/hello", handler)

	m := http.DefaultServeMux
	SetUpBurst(m)
	SetUpItem(m)
	SetUpItemFire(m)
	bigtable.SetUpItemBigtable(m)
	bigtable.SetUpBigtable(m)
	bigeg.SetUpBigItem(m)
	SetUpTaskName(m)
}

func handler(w http.ResponseWriter, r *http.Request) {
	// some code
}
