package catalog

import (
	"errors"
	"toydb/pkg/types"
)

type Catalog struct {
	Database map[string]*DatabaseCatalog
}

type DatabaseCatalog struct {
	Tables map[string]*TableCatalog
}

type TableCatalog struct {
	Name   string
	Schema *types.TableSchema
}

func CreateCatalog(existingData Catalog) (*Catalog, error) {
	return &Catalog{
		Database: existingData.Database,
	}, nil
}

func (c *Catalog) CreateTableSchema(databaseName, tableName string, schema *types.TableSchema) error {
	dbCatalog, ok := c.Database[databaseName]
	if !ok {
		return errors.New("database doesn't exists")
	}
	_, ok = dbCatalog.Tables[tableName]
	if ok {
		return errors.New("table exists already")
	}
	tbCatalog := TableCatalog{
		Name:   tableName,
		Schema: schema,
	}
	dbCatalog.Tables[tableName] = &tbCatalog
	return nil
}

func (c *Catalog) GetTableSchema(databaseName, tableName string) (TableCatalog, error) {
	dbCatalog, ok := c.Database[databaseName]
	if !ok {
		return TableCatalog{}, errors.New("database doesn't exists")
	}
	tableCatalog, ok := dbCatalog.Tables[tableName]
	if !ok {
		return TableCatalog{}, errors.New("table not found")
	}
	return *tableCatalog, nil
}
