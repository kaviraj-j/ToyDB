package catalog

import "toydb/pkg/types"

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
