package proto

// AggregateFunctionDDSketch implements Column interface.
type AggregateFunctionDDSketch struct {
	data    []DDSketch
	colType ColumnType
}

func NewAggregateFunctionDDSketch(colType ColumnType) *AggregateFunctionDDSketch {
	return &AggregateFunctionDDSketch{colType: colType}
}

func (a *AggregateFunctionDDSketch) Append(v DDSketch) {
	a.data = append(a.data, v)
}

func (a *AggregateFunctionDDSketch) AppendArr(v []DDSketch) {
	a.data = append(a.data, v...)
}

func (a AggregateFunctionDDSketch) Row(i int) DDSketch {
	return a.data[i]
}

func (a AggregateFunctionDDSketch) Type() ColumnType { return a.colType }
func (a AggregateFunctionDDSketch) Rows() int        { return len(a.data) }

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

func (a *AggregateFunctionDDSketch) Reset() {
	a.data = nil
}

func (a AggregateFunctionDDSketch) EncodeColumn(b *Buffer) {
	if b == nil {
		return
	}
	for _, v := range a.data {
		v.Encode(b)
	}
}

func (a AggregateFunctionDDSketch) Debug() string {
	var s string
	for i, v := range a.data {
		if i > 0 {
			s += "\n"
		}
		s += v.Debug()
	}
	return s
}
