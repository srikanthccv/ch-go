package proto

import (
	"fmt"
	"strings"
)

// AggregateFunctionDD implements Column interface.
type AggregateFunctionDD struct {
	data []DD
	args []interface{}
	typ  ColumnType
}

// NewAggregateFunctionDD returns new AggregateFunctionDD.
func NewAggregateFunctionDD(args []interface{}, typ ColumnType) *AggregateFunctionDD {
	return &AggregateFunctionDD{args: args, typ: typ}
}

// Append appends DD to column.
func (a *AggregateFunctionDD) Append(v DD) {
	a.data = append(a.data, v)
}

// AppendArr appends DD array to column.
func (a *AggregateFunctionDD) AppendArr(v []DD) {
	a.data = append(a.data, v...)
}

// Row returns DD at index i.
func (a AggregateFunctionDD) Row(i int) DD {
	return a.data[i]
}

// Type returns ColumnTypeAggregateFunction.
func (a AggregateFunctionDD) Type() ColumnType {
	argsStr := make([]string, len(a.args))
	for i, arg := range a.args {
		argsStr[i] = fmt.Sprintf("%v", arg)
	}
	return ColumnType(fmt.Sprintf("%s(%s, %s)", string(ColumnTypeAggregateFunction), strings.Join(argsStr, ", "), a.typ))
}

// Rows returns number of rows in column.
func (a AggregateFunctionDD) Rows() int { return len(a.data) }

// DecodeColumn decodes column from reader.
func (a *AggregateFunctionDD) DecodeColumn(r *Reader, rows int) error {
	for i := 0; i < rows; i++ {
		var v DD
		if err := v.Decode(r); err != nil {
			return err
		}
		a.Append(v)
	}
	return nil
}

// Reset resets column data.
func (a *AggregateFunctionDD) Reset() {
	a.data = nil
}

// EncodeColumn encodes column to buffer.
func (a AggregateFunctionDD) EncodeColumn(b *Buffer) {
	if b == nil {
		return
	}
	for _, v := range a.data {
		v.Encode(b)
	}
}

// Debug returns string representation of column.
func (a AggregateFunctionDD) Debug() string {
	var sketches = make([]string, len(a.data))
	for _, sketch := range a.data {
		sketches = append(sketches, sketch.Debug())
	}
	return strings.Join(sketches, "\n")
}
