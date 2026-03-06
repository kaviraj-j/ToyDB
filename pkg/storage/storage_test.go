package storage_test

import (
	"testing"
	"toydb/pkg/storage"
	"toydb/pkg/types"
)

func TestStorage_CreateAndReadDatabase(t *testing.T) {
	tmpDir := t.TempDir()

	s := &storage.Storage{
		StoragePath: tmpDir,
	}

	dbName := "testdb"
	tableName := "users"

	// create database using API
	if err := s.CreateDatabase(dbName); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	s.CreateTable(dbName, tableName, []types.Column{
		{
			Name: "id",
			Type: types.ValueTypeInt,
		},
		{
			Name: "name",
			Type: types.ValueTypeString,
		},
	})

	s.InsertRows(dbName, tableName, []types.Row{
		[]any{2, "Luffy"},
		[]any{3, "Sanji"},
	})
}
