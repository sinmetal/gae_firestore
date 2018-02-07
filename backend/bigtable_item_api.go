package backend

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/appengine"
	"google.golang.org/appengine/log"

	"cloud.google.com/go/bigtable"
)

const instance = "sample"
const table = "Item"
const family = "myfamily"
const column = "mycolumn"

type ItemBigtableApi struct {
}

type BigtableRow struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func SetUpItemBigtable(m *http.ServeMux) {
	api := ItemBigtableApi{}

	m.HandleFunc("/api/1/itembigtable", api.handler)
}

func (a *ItemBigtableApi) handler(w http.ResponseWriter, r *http.Request) {
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

func (a *ItemBigtableApi) doPost(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	start := time.Now()
	err := updateBigtable(ctx, appengine.AppID(ctx), instance, table, family, column)
	end := time.Now()
	log.Infof(ctx, "{\"bigtable-updateBigtable\":{\"duration\":%d}}", end.Sub(start).Nanoseconds())
	if err != nil {
		log.Errorf(ctx, "updateBigtable: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Done!"))
	return
}

func (a *ItemBigtableApi) doList(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	start := time.Now()
	rows, err := listBigtable(ctx, appengine.AppID(ctx), instance, table, family, column)
	end := time.Now()
	log.Infof(ctx, "{\"bigtable-listBigtable\":{\"duration\":%d}}", end.Sub(start).Nanoseconds())
	if err != nil {
		log.Errorf(ctx, "listBigtable: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	j, err := json.Marshal(rows)
	if err != nil {
		log.Errorf(ctx, "json.Marshal: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write(j)
	return
}

func listBigtable(ctx context.Context, projectID string, instance string, table string, family string, column string) ([]BigtableRow, error) {
	client, err := bigtable.NewClient(ctx, projectID, instance)
	if err != nil {
		return nil, fmt.Errorf("failed Bigtable.NewClient(): projectID=%s, instance=%s", projectID, instance)
	}

	tbl := client.Open(table)

	var rows []BigtableRow
	err = tbl.ReadRows(ctx, bigtable.PrefixRange(column), func(row bigtable.Row) bool {
		item := row[family][0]
		rows = append(rows, BigtableRow{
			Key:   row.Key(),
			Value: string(item.Value),
		})
		return true
	}, bigtable.RowFilter(bigtable.ColumnFilter(column)))

	if err = client.Close(); err != nil {
		return nil, fmt.Errorf("Could not close data operations client: %v", err)
	}

	return rows, nil
}

func updateBigtable(ctx context.Context, projectID string, instance string, table string, family string, column string) error {
	client, err := bigtable.NewClient(ctx, projectID, instance)
	if err != nil {
		return fmt.Errorf("failed Bigtable.NewClient(): projectID=%s, instance=%s", projectID, instance)
	}
	defer client.Close()

	tbl := client.Open(table)
	mut := bigtable.NewMutation()
	mut.Set(family, column, bigtable.Now(), []byte("Hello Bigtable"))
	rowKey := fmt.Sprintf("%s%d", column, time.Now().UnixNano())

	err = tbl.Apply(ctx, rowKey, mut)
	if err != nil {
		return fmt.Errorf("Could not apply bulk row mutation: %v", err)
	}

	return nil
}

// sliceContains reports whether the provided string is present in the given slice of strings.
func sliceContains(list []string, target string) bool {
	for _, s := range list {
		if s == target {
			return true
		}
	}
	return false
}
