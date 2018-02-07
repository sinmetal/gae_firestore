package backend

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"

	"github.com/pborman/uuid"

	"golang.org/x/net/context"
	"google.golang.org/appengine/log"
)

const parentID = "big-mother"

type BigItem struct {
	ID            string
	Title         string
	Description   string
	CreatedAt     time.Time
	UpdatedAt     time.Time
	SchemaVersion int
}

type BigItemApi struct {
}

func SetUpBigItem(m *http.ServeMux) {
	api := BigItemApi{}

	m.HandleFunc("/api/1/bigeg", api.handler)
}

func (a *BigItemApi) handler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		a.doPost(w, r)
	} else {
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func (a *BigItemApi) doPost(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	countParam := r.FormValue("count")
	count, err := strconv.Atoi(countParam)
	if err != nil {
		http.Error(w, "count is number.", http.StatusBadRequest)
		return
	}

	type taba struct {
		keys  []*datastore.Key
		items []BigItem
	}
	var tabas []taba

	parentKey := datastore.NewKey(ctx, "BigItem", parentID, 0, nil)
	var keys []*datastore.Key
	var items []BigItem
	currentTaba := 0
	for i := 0; i < count; i++ {
		id := uuid.New()
		now := time.Now()
		keys = append(keys, datastore.NewKey(ctx, "BigItem", id, 0, parentKey))
		items = append(items, BigItem{
			ID:            id,
			Title:         fmt.Sprintf("Hello Big Item Number %d", i),
			Description:   fmt.Sprintf("とっても大きなEntity Groupを作ろう！僕は %d 番目のEntityだよ！", i),
			CreatedAt:     now,
			UpdatedAt:     now,
			SchemaVersion: 1,
		})
		currentTaba++
		if currentTaba >= 500 {
			tabas = append(tabas, taba{
				keys,
				items,
			})
			keys = []*datastore.Key{}
			items = []BigItem{}
			currentTaba = 0
		}
	}

	// 3000件ぐらいが限度だった
	err = datastore.RunInTransaction(ctx, func(ctx context.Context) error {
		for _, taba := range tabas {
			_, err := datastore.PutMulti(ctx, taba.keys, taba.items)
			if err != nil {
				return fmt.Errorf("datastore.PutMulti. %v", err)
			}
		}

		return nil
	}, nil)
	if err != nil {
		log.Errorf(ctx, "%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Done!"))
}
