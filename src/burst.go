package gae_firestore

import (
	"net/http"

	"fmt"
	"google.golang.org/appengine"
	"google.golang.org/appengine/taskqueue"
	"strconv"
)

type BurstApi struct {
}

func SetUpBurst(m *http.ServeMux) {
	api := BurstApi{}

	m.HandleFunc("/admin/1/burst", api.handler)
}

func (a *BurstApi) handler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	url := r.FormValue("url")
	method := r.FormValue("method")
	loopCountParam := r.FormValue("loopCount")
	burstQueueCountParam := r.FormValue("burstQueueCount")

	if len(url) < 1 {
		http.Error(w, "url is required.", http.StatusBadRequest)
		return
	}

	var err error

	loopCount := 100
	if len(loopCountParam) > 0 {
		loopCount, err = strconv.Atoi(loopCountParam)
		if err != nil {
			http.Error(w, "loopCount is int.", http.StatusBadRequest)
			return
		}
	}

	burstQueueCount := 1
	if len(burstQueueCountParam) > 0 {
		burstQueueCount, err = strconv.Atoi(burstQueueCountParam)
		if err != nil {
			http.Error(w, "burstQueueCount is int.", http.StatusBadRequest)
			return
		}
	}

	for i := 0; i < loopCount; i++ {
		var tasks []*taskqueue.Task
		for j := 0; j < 100; j++ {
			tasks = append(tasks, &taskqueue.Task{
				Path:   url,
				Method: method,
			})
		}
		for burst := 1; burst < burstQueueCount+1; burst++ {
			_, err := taskqueue.AddMulti(ctx, tasks, fmt.Sprintf("burst%d", burst))
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Burst Start!!"))
}
