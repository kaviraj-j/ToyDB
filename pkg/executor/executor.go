package executor

import (
	"fmt"
	"math/rand"
	"toydb/pkg/planner"
	"toydb/pkg/storage"
	"toydb/pkg/types"
)

type Executor struct {
	storage *storage.Storage
	dbName  string
}

func NewExecutor(s *storage.Storage, dbName string) *Executor {
	return &Executor{storage: s, dbName: dbName}
}

func (e *Executor) Build(plan planner.PlanNode, schema *types.TableSchema) (ExecutorNode, error) {
	switch p := plan.(type) {

	case planner.SeqScanPlan:
		return NewSeqScanExec(e.dbName, p.TableName, e.storage)

	case planner.FilterPlan:
		child, err := e.Build(p.Child, schema)
		if err != nil {
			return nil, err
		}
		return NewFilterExec(p.Expr, p.Schema, child)

	case planner.ProjectionPlan:
		child, err := e.Build(p.Child, schema)
		if err != nil {
			return nil, err
		}
		return NewProjectionExecutor(p.Schema, p.Columns, child)

	default:
		return nil, fmt.Errorf("unknown plan node: %T", plan)
	}
}

type ExecutorNode interface {
	Next() (*types.Row, error)
}

type SeqScanExecutor struct {
	databaseName string
	tableName    string
	cursor       int // index of the current item in storage
	storage      *storage.Storage
	cachedRows   []types.Row
}

func NewSeqScanExec(databaseName, tableName string, s *storage.Storage) (ExecutorNode, error) {
	return &SeqScanExecutor{
		databaseName: databaseName,
		tableName:    tableName,
		cursor:       0,
		storage:      s,
	}, nil
}

func (e *SeqScanExecutor) Next() (*types.Row, error) {
	// find the row
	rows := e.cachedRows
	if rows == nil {
		rows, err := e.storage.GetRows(e.databaseName, e.tableName)
		if err != nil || len(rows) <= e.cursor {
			return nil, err
		}
		e.cachedRows = rows
	}
	row := rows[e.cursor]
	e.cursor++
	return &row, nil
}

type FilterExecutor struct {
	expr   types.Expression
	schema *types.TableSchema
	child  ExecutorNode
}

func NewFilterExec(expr types.Expression, schema *types.TableSchema, child ExecutorNode) (ExecutorNode, error) {
	return &FilterExecutor{
		expr:   expr,
		schema: schema,
		child:  child,
	}, nil
}

func (e *FilterExecutor) Next() (*types.Row, error) {
	for {
		row, err := e.child.Next()
		if row == nil || err != nil {
			return row, err
		}
		if matchingRecord(e.expr, *row) {
			return row, nil
		}
	}
}

func matchingRecord(expr any, row types.Row) bool {
	// TODO: implement matching record with Expression handlers
	n := rand.Intn(10)
	return n%2 == 0
}

type ProjectionExecutor struct {
	columns []types.Column
	schema  *types.TableSchema
	child   ExecutorNode
}

func NewProjectionExecutor(schema *types.TableSchema, columns []types.Column, child ExecutorNode) (ExecutorNode, error) {
	return &ProjectionExecutor{
		columns: columns,
		schema:  schema,
		child:   child,
	}, nil
}

func (e *ProjectionExecutor) Next() (*types.Row, error) {
	// TODO: return only the necessary columns
	return e.child.Next()
}
