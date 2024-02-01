package proto

import (
	"fmt"
	"strings"
)

// AggregateFunctionDDSketch implements Column interface.
type AggregateFunctionDDSketch struct {
	data []DDSketch
	args []interface{}
	typ  ColumnType
}

// NewAggregateFunctionDDSketch returns new AggregateFunctionDDSketch.
func NewAggregateFunctionDDSketch(args []interface{}, typ ColumnType) *AggregateFunctionDDSketch {
	return &AggregateFunctionDDSketch{args: args, typ: typ}
}

// Append appends DDSketch to column.
func (a *AggregateFunctionDDSketch) Append(v DDSketch) {
	a.data = append(a.data, v)
}

// AppendArr appends DDSketch array to column.
func (a *AggregateFunctionDDSketch) AppendArr(v []DDSketch) {
	a.data = append(a.data, v...)
}

// Row returns DDSketch at index i.
func (a AggregateFunctionDDSketch) Row(i int) DDSketch {
	return a.data[i]
}

// Type returns ColumnTypeAggregateFunction.
func (a AggregateFunctionDDSketch) Type() ColumnType {
	argsStr := make([]string, len(a.args))
	for i, arg := range a.args {
		argsStr[i] = fmt.Sprintf("%v", arg)
	}
	return ColumnType(fmt.Sprintf("%s(%s, %s)", string(ColumnTypeAggregateFunction), strings.Join(argsStr, ", "), a.typ))
}

// Rows returns number of rows in column.
func (a AggregateFunctionDDSketch) Rows() int { return len(a.data) }

// DecodeColumn decodes column from reader.
func (a *AggregateFunctionDDSketch) DecodeColumn(r *Reader, rows int) error {
	for i := 0; i < rows; i++ {
		var v DDSketch
		if err := v.Decode(r); err != nil {
			return err
		}
		a.Append(v)
	}
	return nil
}

// Reset resets column data.
func (a *AggregateFunctionDDSketch) Reset() {
	a.data = nil
}

// EncodeColumn encodes column to buffer.
func (a AggregateFunctionDDSketch) EncodeColumn(b *Buffer) {
	if b == nil {
		return
	}
	for _, v := range a.data {
		v.Encode(b)
	}
}

// Debug returns string representation of column.
func (a AggregateFunctionDDSketch) Debug() string {
	var sketches = make([]string, len(a.data))
	for _, sketch := range a.data {
		sketches = append(sketches, sketch.Debug())
	}
	return strings.Join(sketches, "\n")
}
