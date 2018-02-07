package backend

import (
	"fmt"
	"net/http"

	"golang.org/x/net/context"

	"google.golang.org/appengine"
	"google.golang.org/appengine/log"

	"cloud.google.com/go/bigtable"
)

type BigtableApi struct {
}

func SetUpBigtable(m *http.ServeMux) {
	api := BigtableApi{}

	m.HandleFunc("/api/1/bigtable", api.handler)
}

func (a *BigtableApi) handler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		a.doPost(w, r)
	} else if r.Method == "GET" {
		http.Error(w, "", http.StatusMethodNotAllowed)
	} else if r.Method == "PUT" {
		http.Error(w, "", http.StatusMethodNotAllowed)
	} else if r.Method == "DELETE" {
		http.Error(w, "", http.StatusMethodNotAllowed)
	} else {
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func (a *BigtableApi) doPost(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	err := createTableWithColumnFamily(ctx, appengine.AppID(ctx), instance, table, family)
	if err != nil {
		log.Errorf(ctx, "createBigtable: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Done!"))
	return
}

func createTableWithColumnFamily(ctx context.Context, project string, instance string, table string, columnFamily string) error {
	adminClient, err := bigtable.NewAdminClient(ctx, project, instance)
	if err != nil {
		return fmt.Errorf("Could not create admin client: project=%s, instance=%s : %v", project, instance, err)
	}
	defer adminClient.Close()

	tables, err := adminClient.Tables(ctx)
	if err != nil {
		return fmt.Errorf("Could not fetch table list: project=%s, instance=%s : %v", project, instance, err)
	}

	if !sliceContains(tables, table) {
		if err := adminClient.CreateTable(ctx, table); err != nil {
			return fmt.Errorf("Could not create table %s: %v", table, err)
		}
	}

	tblInfo, err := adminClient.TableInfo(ctx, table)
	if err != nil {
		return fmt.Errorf("Could not read info for table %s: %v", table, err)
	}

	if !sliceContains(tblInfo.Families, columnFamily) {
		if err := adminClient.CreateColumnFamily(ctx, table, columnFamily); err != nil {
			return fmt.Errorf("Could not create column family %s: %v", columnFamily, err)
		}
	}

	return nil
}
