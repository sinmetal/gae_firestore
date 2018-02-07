package backend

import (
	"net/http"
)

func init() {
	http.HandleFunc("/hello", handler)

	m := http.DefaultServeMux
	SetUpBurst(m)
	SetUpItem(m)
	SetUpItemFire(m)
	SetUpItemBigtable(m)
	SetUpBigtable(m)
	SetUpBigItem(m)
}

func handler(w http.ResponseWriter, r *http.Request) {
	// some code
}
