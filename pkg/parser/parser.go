package parser

type Statement interface {
	statement()
}

type SelectStatement struct {
}

// ==== marker methods ====
func (SelectStatement) statement() {}
