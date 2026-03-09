package types

type ValueType int

const (
	ValueTypeInt ValueType = iota
	ValueTypeString
	ValueTypeBool
)

type Column struct {
	Name string    `json:"name"`
	Type ValueType `json:"type"`
}

type Row []any

type TableSchema struct {
	Columns []Column `json:"name"`
}

type Expression any
