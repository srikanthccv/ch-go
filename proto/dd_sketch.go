package proto

import (
	"errors"
	"strconv"
)

// Compatible with ddsketch protobuf model.

var (
	ErrInvalidFlag = errors.New("invalid flag")
)

type DDSketch struct {
	Mapping        *IndexMapping
	PositiveValues *Store
	NegativeValues *Store
	ZeroCount      float64
}

func (d DDSketch) Encode(b *Buffer) {
	if b == nil {
		return
	}
	b.PutByte(FlagIndexMappingBaseLogarithmic.byte)
	d.Mapping.Encode(b)
	b.PutByte(FlagTypePositiveStore.byte)
	d.PositiveValues.Encode(b)
	b.PutByte(FlagTypeNegativeStore.byte)
	d.NegativeValues.Encode(b)
	b.PutByte(FlagZeroCountVarFloat.byte)
	b.PutFloat64(d.ZeroCount)
}

func (d *DDSketch) Decode(r *Reader) error {
	flag, err := r.Byte()
	if err != nil {
		return err
	}
	if flag != FlagIndexMappingBaseLogarithmic.byte {
		return ErrInvalidFlag
	}
	d.Mapping = &IndexMapping{}
	if err := d.Mapping.Decode(r); err != nil {
		return err
	}
	flag, err = r.Byte()
	if err != nil {
		return err
	}
	if flag != FlagTypePositiveStore.byte {
		return ErrInvalidFlag
	}
	d.PositiveValues = &Store{}
	if err := d.PositiveValues.Decode(r); err != nil {
		return err
	}
	flag, err = r.Byte()
	if err != nil {
		return err
	}
	if flag != FlagTypeNegativeStore.byte {
		return ErrInvalidFlag
	}
	d.NegativeValues = &Store{}
	if err := d.NegativeValues.Decode(r); err != nil {
		return err
	}
	flag, err = r.Byte()
	if err != nil {
		return err
	}
	if flag != FlagZeroCountVarFloat.byte {
		return ErrInvalidFlag
	}
	zeroCount, err := r.Float64()
	if err != nil {
		return err
	}
	d.ZeroCount = zeroCount
	return nil
}

func (d DDSketch) Debug() string {
	var s string
	s += "Mapping:\n"
	s += d.Mapping.Debug()
	s += "\nPositive values:\n"
	s += d.PositiveValues.Debug()
	s += "\nNegative values:\n"
	s += d.NegativeValues.Debug()
	s += "\nZero count: "
	s += strconv.FormatFloat(d.ZeroCount, 'f', -1, 64)
	return s
}

type IndexMapping struct {
	Gamma float64

	IndexOffset float64
}

func (m IndexMapping) Encode(b *Buffer) {
	if b == nil {
		return
	}
	b.PutFloat64(m.Gamma)
	b.PutFloat64(m.IndexOffset)
}

func (m *IndexMapping) Decode(r *Reader) error {
	gamma, err := r.Float64()
	if err != nil {
		return err
	}
	m.Gamma = gamma
	indexOffset, err := r.Float64()
	if err != nil {
		return err
	}
	m.IndexOffset = indexOffset
	return nil
}

func (m IndexMapping) Debug() string {
	var s string
	s += "Gamma: "
	s += strconv.FormatFloat(m.Gamma, 'f', -1, 64)
	s += "\nIndex offset: "
	s += strconv.FormatFloat(m.IndexOffset, 'f', -1, 64)
	return s
}

type Store struct {
	BinCounts map[int32]float64

	ContiguousBinCounts      []float64
	ContiguousBinIndexOffset int32
}

func (s Store) Encode(b *Buffer) {
	if b == nil {
		return
	}
	if len(s.ContiguousBinCounts) > 0 {
		b.PutByte(BinEncodingContiguousCounts.byte)
		b.PutUVarInt(uint64(len(s.ContiguousBinCounts)))
		b.PutVarInt(int64(s.ContiguousBinIndexOffset))
		b.PutVarInt(1)
		for _, v := range s.ContiguousBinCounts {
			b.PutFloat64(v)
		}
	} else {
		b.PutByte(BinEncodingIndexDeltasAndCounts.byte)
		b.PutUVarInt(uint64(len(s.BinCounts)))
		for k, v := range s.BinCounts {
			b.PutVarInt(int64(k))
			b.PutFloat64(v)
		}
	}
}

func (s *Store) Decode(r *Reader) error {
	encoding, err := r.Byte()
	if err != nil {
		return err
	}
	if encoding == BinEncodingContiguousCounts.byte {
		count, err := r.UVarInt()
		if err != nil {
			return err
		}
		contiguousBinIndexOffset, err := r.VarInt()
		if err != nil {
			return err
		}
		_, err = r.VarInt()
		if err != nil {
			return err
		}
		s.ContiguousBinCounts = make([]float64, count)
		for i := range s.ContiguousBinCounts {
			v, err := r.Float64()
			if err != nil {
				return err
			}
			s.ContiguousBinCounts[i] = v
		}
		s.ContiguousBinIndexOffset = int32(contiguousBinIndexOffset)
	} else {
		count, err := r.UVarInt()
		if err != nil {
			return err
		}
		s.BinCounts = make(map[int32]float64, count)
		for i := uint64(0); i < count; i++ {
			k, err := r.VarInt()
			if err != nil {
				return err
			}
			v, err := r.Float64()
			if err != nil {
				return err
			}
			s.BinCounts[int32(k)] = v
		}
	}
	return nil
}

func (store Store) Debug() string {
	var s string
	if len(store.ContiguousBinCounts) > 0 {
		s += "Contiguous bin counts:\n"
		for i, v := range store.ContiguousBinCounts {
			s += strconv.Itoa(int(store.ContiguousBinIndexOffset) + i)
			s += ": "
			s += strconv.FormatFloat(v, 'f', -1, 64)
			s += ", "
		}
	} else {
		s += "Bin counts:\n"
		for k, v := range store.BinCounts {
			s += strconv.Itoa(int(k))
			s += ": "
			s += strconv.FormatFloat(v, 'f', -1, 64)
			s += ", "
		}
	}
	return s
}
