package storage

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"toydb/pkg/catalog"
	"toydb/pkg/types"
)

type Storage struct {
	StoragePath string
}

type Database struct {
	Name    string                       `json:"name"`
	Schemas map[string]types.TableSchema `json:"schemas"`
	Tables  []Table                      `json:"tables"`
}

type Table struct {
	Name string      `json:"name"`
	Data []types.Row `json:"data"`
}

func NewStore(storagePath string) (*Storage, error) {
	err := validateDir(storagePath)

	return &Storage{
		StoragePath: storagePath,
	}, err
}

func (s *Storage) GetCatalog() (catalog.Catalog, error) {
	databaseNames := make([]string, 0)

	files, err := os.ReadDir(s.StoragePath)
	if err != nil {
		return catalog.Catalog{}, err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		name := file.Name()

		if strings.HasSuffix(name, ".db.json") {
			dbName := strings.TrimSuffix(name, ".db.json")
			databaseNames = append(databaseNames, dbName)
		}
	}

	var c catalog.Catalog
	for _, dbName := range databaseNames {
		db, _ := s.getDB(dbName)
		dbCatalog := catalog.DatabaseCatalog{
			Tables: make(map[string]*catalog.TableCatalog),
		}
		for tName, schema := range db.Schemas {
			dbCatalog.Tables[tName] = &catalog.TableCatalog{
				Name:   tName,
				Schema: &schema,
			}
		}
		c.Database[dbName] = &dbCatalog
	}
	return c, nil
}

//
// ================= DATABASE =================
//

func (s *Storage) CreateDatabase(databaseName string) error {
	_, err := s.getDB(databaseName)
	if err == nil {
		return errors.New("database already exists")
	}

	db := Database{
		Name:    databaseName,
		Schemas: make(map[string]types.TableSchema),
		Tables:  []Table{},
	}

	return s.saveDB(db)
}

//
// ================= TABLE =================
//

// CREATE TABLE
func (s *Storage) CreateTable(
	databaseName string,
	tableName string,
	columns []types.Column,
) error {

	db, err := s.getDB(databaseName)
	if err != nil {
		return err
	}

	if _, ok := db.Schemas[tableName]; ok {
		return errors.New("table already exists")
	}

	db.Schemas[tableName] = types.TableSchema{
		Columns: columns,
	}

	db.Tables = append(db.Tables, Table{
		Name: tableName,
		Data: []types.Row{},
	})

	return s.saveDB(db)
}

// DROP TABLE
func (s *Storage) DropTable(databaseName string, tableName string) error {

	db, err := s.getDB(databaseName)
	if err != nil {
		return err
	}

	if _, ok := db.Schemas[tableName]; !ok {
		return errors.New("table does not exist")
	}

	delete(db.Schemas, tableName)

	updated := make([]Table, 0, len(db.Tables))
	for _, t := range db.Tables {
		if t.Name != tableName {
			updated = append(updated, t)
		}
	}

	db.Tables = updated

	return s.saveDB(db)
}

//
// ================= ROW OPERATIONS =================
//

// SELECT * FROM table
func (s *Storage) GetRows(
	databaseName string,
	tableName string,
) ([]types.Row, error) {

	db, err := s.getDB(databaseName)
	if err != nil {
		return nil, err
	}

	idx, err := db.getTableIndex(tableName)
	if err != nil {
		return nil, err
	}

	rows := db.Tables[idx].Data

	// return copy (avoid external mutation)
	out := make([]types.Row, len(rows))
	copy(out, rows)

	return out, nil
}

// INSERT INTO table VALUES (...)
func (s *Storage) InsertRows(
	databaseName string,
	tableName string,
	rows []types.Row,
) error {

	db, err := s.getDB(databaseName)
	if err != nil {
		return err
	}

	schema, ok := db.Schemas[tableName]
	if !ok {
		return errors.New("table schema not found")
	}

	idx, err := db.getTableIndex(tableName)
	if err != nil {
		return err
	}

	table := &db.Tables[idx]

	for _, r := range rows {

		if len(r) != len(schema.Columns) {
			return errors.New("column count mismatch")
		}

		table.Data = append(table.Data, []any(r))
	}

	return s.saveDB(db)
}

// UPDATE table SET ...
func (s *Storage) UpdateRows(
	databaseName string,
	tableName string,
	rowIndexes []int,
	updater func([]any) []any,
) error {

	db, err := s.getDB(databaseName)
	if err != nil {
		return err
	}

	idx, err := db.getTableIndex(tableName)
	if err != nil {
		return err
	}

	table := &db.Tables[idx]

	for _, ri := range rowIndexes {

		if ri < 0 || ri >= len(table.Data) {
			return errors.New("row index out of bounds")
		}

		table.Data[ri] = updater(table.Data[ri])
	}

	return s.saveDB(db)
}

// DELETE FROM table WHERE ...
func (s *Storage) DeleteRows(
	databaseName string,
	tableName string,
	rowIndexes []int,
) error {

	db, err := s.getDB(databaseName)
	if err != nil {
		return err
	}

	idx, err := db.getTableIndex(tableName)
	if err != nil {
		return err
	}

	table := &db.Tables[idx]

	// delete backwards to avoid index shifting
	for i := len(rowIndexes) - 1; i >= 0; i-- {

		ri := rowIndexes[i]

		if ri < 0 || ri >= len(table.Data) {
			return errors.New("row index out of bounds")
		}

		table.Data = append(
			table.Data[:ri],
			table.Data[ri+1:]...,
		)
	}

	return s.saveDB(db)
}

//
// ================= HELPERS =================
//

func (s *Storage) getDB(databaseName string) (Database, error) {
	var db Database

	data, err := os.ReadFile(
		filepath.Join(s.StoragePath, databaseName+".db.json"),
	)
	if err != nil {
		return db, err
	}

	err = json.Unmarshal(data, &db)
	return db, err
}

func (s *Storage) saveDB(database Database) error {

	data, err := json.MarshalIndent(database, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(
		filepath.Join(s.StoragePath, database.Name+".db.json"),
		data,
		0o644,
	)
}

func validateDir(path string) error {
	info, err := os.Stat(path)
	if err == nil && !info.IsDir() {
		return errors.New("storage path is not a directory")
	}
	return err
}

func (db *Database) getTableIndex(tableName string) (int, error) {
	for i := range db.Tables {
		if db.Tables[i].Name == tableName {
			return i, nil
		}
	}
	return -1, errors.New("table not found")
}
