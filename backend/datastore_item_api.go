package backend

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"net/http"
	"time"

	"github.com/pborman/uuid"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/memcache"
	"google.golang.org/appengine/taskqueue"
)

// Item
type Item struct {
	KeyStr    string    `json:"key" datastore:"-" firestore:"-"`
	Title     string    `json:"title" datastore:",noindex"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type ItemApi struct {
}

func SetUpItem(m *http.ServeMux) {
	api := ItemApi{}

	m.HandleFunc("/api/1/item", api.handler)
	m.HandleFunc("/queue/1/item", api.handlerQueue)
}

func (a *ItemApi) handler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	if r.Method == "POST" {
		a.doPost(w, r)
	} else if r.Method == "GET" {
		key := r.URL.Query().Get("key")
		log.Infof(c, "key param = %s", key)

		if len(key) < 1 {
			a.doList(w, r)
		} else {
			a.doGet(w, r)
		}
	} else if r.Method == "PUT" {
		a.doPut(w, r)
	} else if r.Method == "DELETE" {
		a.doDelete(w, r)
	} else {
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func (a *ItemApi) handlerQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		a.doPostByQueue(w, r)
	} else {
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func (a *ItemApi) doPost(w http.ResponseWriter, r *http.Request) {
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

	founderKey := datastore.NewKey(c, "Item", "@founder@", 0, nil)
	// datastore.RunInTransactionは自動的にリトライされるので、冪等性を考慮して、UUIDは先に作っておく
	id := uuid.New()
	err = datastore.RunInTransaction(c, func(c context.Context) error {
		key, err := datastore.Put(c, datastore.NewKey(c, "Item", id, 0, founderKey), &item)
		if err != nil {
			log.Warningf(c, "%v", err)
			return err
		}
		item.KeyStr = key.Encode()

		t := taskqueue.NewPOSTTask("/queue/1/item", map[string][]string{"key": {item.KeyStr}})
		if _, err := taskqueue.Add(c, t, "item-after"); err != nil {
			log.Warningf(c, "%v", err)
			return err
		}

		return nil
	}, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(item)
}

func (a *ItemApi) doList(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	q := datastore.NewQuery("Item").Order("-UpdatedAt").Limit(10)

	var items []Item
	t := q.Run(c)
	for {
		var item Item
		k, err := t.Next(&item)
		if err == datastore.Done {
			break
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		item.KeyStr = k.Encode()
		items = append(items, item)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=60") // add edge cache
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(items)
}

func (a *ItemApi) doGet(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	keyStr := r.URL.Query().Get("key")
	if mi, err := memcache.Get(c, keyStr); err == memcache.ErrCacheMiss {
		// nop
	} else if err != nil {
		log.Warningf(c, "item get memcache error, %s", err.Error())
	} else {
		log.Infof(c, "memcache hit!")
		item := Item{}
		err = item.GobDecode(mi.Value)
		if err != nil {
			log.Warningf(c, "item get memcache decode error, %s", err.Error())
		} else {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(item)
			return
		}
	}

	key, err := datastore.DecodeKey(keyStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var item Item
	err = datastore.Get(c, key, &item)
	if err == datastore.ErrNoSuchEntity {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	item.KeyStr = key.Encode()

	b, err := item.GobEncode()
	if err != nil {
		log.Warningf(c, "item gob encode error, %s", err.Error())
	}
	mi := &memcache.Item{
		Key:   item.KeyStr,
		Value: b,
	}
	err = memcache.Add(c, mi)
	if err != nil {
		log.Warningf(c, "item add memcache error, %s", err.Error())
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(item)
}

func (a *ItemApi) doPut(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	keyStr := r.URL.Query().Get("key")
	if len(keyStr) < 1 {
		http.Error(w, "required key.", http.StatusBadRequest)
		return
	}
	key, err := datastore.DecodeKey(keyStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var param Item
	err = json.NewDecoder(r.Body).Decode(&param)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	err = memcache.Delete(c, keyStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var item Item
	err = datastore.RunInTransaction(c, func(c context.Context) error {
		err := datastore.Get(c, key, &item)
		if err != nil {
			return err
		}
		item.Title = param.Title
		item.UpdatedAt = time.Now()

		_, err = datastore.Put(c, key, &item)
		return err
	}, nil)
	if err == datastore.ErrNoSuchEntity {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	item.KeyStr = key.Encode()

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(item)
}

func (a *ItemApi) doDelete(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	keyStr := r.FormValue("key")
	if len(keyStr) < 1 {
		http.Error(w, "required key.", http.StatusBadRequest)
		return
	}
	key, err := datastore.DecodeKey(keyStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = memcache.Delete(c, keyStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = datastore.Delete(c, key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
}

func (a *ItemApi) doPostByQueue(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	// exampleとしてHeader表示
	for k, v := range r.Header {
		log.Infof(c, "%s:%s", k, v)
	}

	keyStr := r.FormValue("key")
	log.Infof(c, "key param = %s", keyStr)
	key, err := datastore.DecodeKey(keyStr)
	if err != nil {
		log.Errorf(c, "key decode error = %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var item Item
	err = datastore.Get(c, key, &item)
	if err == datastore.ErrNoSuchEntity {
		log.Errorf(c, "entity not found = %s", err.Error())
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		log.Errorf(c, "entity get error = %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	item.KeyStr = key.Encode()

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(item)
}

func (item *Item) GobEncode() ([]byte, error) {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	err := encoder.Encode(item.KeyStr)
	if err != nil {
		return nil, err
	}
	err = encoder.Encode(item.Title)
	if err != nil {
		return nil, err
	}
	err = encoder.Encode(item.CreatedAt)
	if err != nil {
		return nil, err
	}
	err = encoder.Encode(item.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (item *Item) GobDecode(buf []byte) error {
	r := bytes.NewBuffer(buf)
	decoder := gob.NewDecoder(r)
	err := decoder.Decode(&item.KeyStr)
	if err != nil {
		return err
	}
	err = decoder.Decode(&item.Title)
	if err != nil {
		return err
	}
	err = decoder.Decode(&item.CreatedAt)
	if err != nil {
		return err
	}
	return decoder.Decode(&item.UpdatedAt)
}
