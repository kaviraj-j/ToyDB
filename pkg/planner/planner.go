package planner

import (
	"toydb/pkg/parser"
	"toydb/pkg/types"
)

type Planner struct {
	dbName string
}

func (p *Planner) Plan(stmt parser.Statement) (PlanNode, error)

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
