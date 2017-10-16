package gae_firestore

import (
	"fmt"
	"net/http"

	"golang.org/x/net/context"

	"google.golang.org/appengine"
	"google.golang.org/appengine/log"

	"cloud.google.com/go/bigtable"
)

type ItemBigtableApi struct {
}

func SetUpItemBigtable(m *http.ServeMux) {
	api := ItemBigtableApi{}

	m.HandleFunc("/api/1/itembigtable", api.handler)
}

func (a *ItemBigtableApi) handler(w http.ResponseWriter, r *http.Request) {
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

func (a *ItemBigtableApi) doPost(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	const instance = "sample"
	const table = "Item"
	const family = "myfamily"

	err := createBigtable(ctx, appengine.AppID(ctx), instance, table, family)
	if err != nil {
		log.Errorf(ctx, "createBigtable: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = updateBigtable(ctx, appengine.AppID(ctx), instance, table, family, "mycolumn")
	if err != nil {
		log.Errorf(ctx, "updateBigtable: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Done!"))
	return
}

func updateBigtable(ctx context.Context, projectID string, instance string, table string, family string, column string) error {
	client, err := bigtable.NewClient(ctx, projectID, instance)
	if err != nil {
		return fmt.Errorf("failed Bigtable.NewClient(): projectID=%s, instance=%s", projectID, instance)
	}
	tbl := client.Open(table)
	rmw := bigtable.NewReadModifyWrite()
	rmw.Increment(family, column, 1)
	_, err = tbl.ApplyReadModifyWrite(ctx, column, rmw)
	if err != nil {
		return fmt.Errorf("failed Bigtable.Update(): projectID=%s, instance=%s, table=%s, family=%s, column=%s: %s", projectID, instance, table, family, column, err.Error())
	}

	return nil
}

func createBigtable(ctx context.Context, project string, instance string, table string, columnFamily string) error {
	adminClient, err := bigtable.NewAdminClient(ctx, project, instance)
	if err != nil {
		return fmt.Errorf("Could not create admin client: project=%s, instance=%s : %v", project, instance, err)
	}

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

// sliceContains reports whether the provided string is present in the given slice of strings.
func sliceContains(list []string, target string) bool {
	for _, s := range list {
		if s == target {
			return true
		}
	}
	return false
}
