package gae_firestore

import (
	"io/ioutil"
	"net/http"

	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

type TaskNameApi struct {
}

func SetUpTaskName(m *http.ServeMux) {
	api := TaskNameApi{}

	m.HandleFunc("/api/1/taskname", api.handler)
}

func (a *TaskNameApi) handler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		a.doPost(w, r)
	} else if r.Method == "GET" {
		a.doGet(w, r)
	} else if r.Method == "PUT" {
		http.Error(w, "", http.StatusMethodNotAllowed)
	} else if r.Method == "DELETE" {
		http.Error(w, "", http.StatusMethodNotAllowed)
	} else {
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func (a *TaskNameApi) doPost(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	queuename := r.FormValue("queuename")
	log.Infof(c, "queuename = %s", queuename)

	taskname := r.FormValue("taskname")
	log.Infof(c, "taskname = %s", taskname)

	t := taskqueue.Task{
		Path:   "/api/1/taskname",
		Method: "GET",
		Name:   taskname,
	}

	_, err := taskqueue.Add(c, &t, queuename)
	if err != nil {
		log.Errorf(c, "%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (a *TaskNameApi) doGet(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	for k, v := range r.Header {
		log.Infof(c, "%s:%s", k, v)
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorf(c, "ERROR request body read: %s", err)
		log.Errorf(c, "ERROR task queue add: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Infof(c, string(body))

	w.WriteHeader(http.StatusOK)
}
