package parser

import "toydb/pkg/types"

type Statement interface {
	statement()
}

type SelectStatement struct {
	Columns   []string
	SelectAll bool
	TableName string
	Where     types.Expression
}

// ==== marker methods ====
func (SelectStatement) statement() {}
