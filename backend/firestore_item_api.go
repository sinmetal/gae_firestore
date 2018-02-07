package backend

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/pborman/uuid"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
)

type ItemFireApi struct {
}

func SetUpItemFire(m *http.ServeMux) {
	api := ItemFireApi{}

	m.HandleFunc("/api/1/itemfire", api.handler)
}

func (a *ItemFireApi) handler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		a.doPost(w, r)
	} else if r.Method == "GET" {
		a.doList(w, r)
	} else if r.Method == "PUT" {
		http.Error(w, "", http.StatusMethodNotAllowed)
	} else if r.Method == "DELETE" {
		http.Error(w, "", http.StatusMethodNotAllowed)
	} else {
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func (a *ItemFireApi) doPost(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	var param Item
	err := json.NewDecoder(r.Body).Decode(&param)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	item := Item{
		Title:     param.Title,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	client, err := firestore.NewClient(c, appengine.AppID(c))
	if err != nil {
		log.Errorf(c, "firestore.NewClient: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	id := uuid.New()
	item.KeyStr = id
	doc := client.Doc(fmt.Sprintf("ItemFire/%s", id))

	_, err = doc.Create(c, &item)
	if err != nil {
		log.Errorf(c, "firestore.Document.Create: path = %v, err = %v", doc.Path, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(item)
}

func (a *ItemFireApi) doList(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	client, err := firestore.NewClient(c, appengine.AppID(c))
	if err != nil {
		log.Errorf(c, "firestore.NewClient: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	cr := client.Collection("ItemFire")
	dss, err := cr.Documents(c).GetAll()
	if err != nil {
		log.Errorf(c, "firestore.Document.Collection: path = %v, err = %v", "ItemFire", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var items []Item
	for _, ds := range dss {
		var item Item
		err := ds.DataTo(&item)
		if err != nil {
			log.Errorf(c, "firestore.Document.DataTo: path = %v, err = %v", ds.Ref.Path, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		item.KeyStr = ds.Ref.ID
		items = append(items, item)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(items)
}
