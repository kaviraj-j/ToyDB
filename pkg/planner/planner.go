package planner

import (
	"errors"
	"slices"
	"toydb/pkg/catalog"
	"toydb/pkg/parser"
	"toydb/pkg/types"
)

type Planner struct {
	DBName  string
	Catalog *catalog.Catalog
}

func (p *Planner) Plan(stmt parser.Statement) (PlanNode, error) {
	switch s := stmt.(type) {
	case parser.SelectStatement:
		var plan PlanNode
		scanPlan := SeqScanPlan{
			DatabaseName: p.DBName,
			TableName:    s.TableName,
		}
		plan = scanPlan
		tableCatalog, _ := p.Catalog.GetTableSchema(p.DBName, s.TableName)
		// TODO: check for `Where` existance
		if s.Where != 0 {

			plan = FilterPlan{
				Expr:   s.Where,
				Schema: tableCatalog.Schema,
				Child:  scanPlan,
			}
		}
		var columns []types.Column
		if s.SelectAll {
			columns = make([]types.Column, len(tableCatalog.Schema.Columns))
			copy(columns, tableCatalog.Schema.Columns)
		} else {
			for _, c := range tableCatalog.Schema.Columns {
				if slices.Index(s.Columns, c.Name) != -1 {
					columns = append(columns, c)
				}
			}
		}
		plan = ProjectionPlan{
			Columns: columns,
			Schema:  tableCatalog.Schema,
			Child:   plan,
		}
		return plan, nil
	default:
		return FilterPlan{}, errors.New("invalid statement type")
	}

}

type PlanNode interface {
	planNode()
}

type SeqScanPlan struct {
	DatabaseName string
	TableName    string
}

type FilterPlan struct {
	Expr   types.Expression
	Schema *types.TableSchema
	Child  PlanNode
}

type ProjectionPlan struct {
	Columns []types.Column
	Schema  *types.TableSchema
	Child   PlanNode
}

// ===== marker methods =====
func (SeqScanPlan) planNode()    {}
func (FilterPlan) planNode()     {}
func (ProjectionPlan) planNode() {}
