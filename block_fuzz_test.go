package ch

import (
	"bytes"
	"net/netip"
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go/internal/gold"
	"github.com/ClickHouse/ch-go/proto"
)

func encodeTestStrBlock() []byte {
	b := &proto.Buffer{}
	arr := new(proto.ColStr).Array()
	for _, v := range [][]string{
		{"foo", "bar"},
		{"1", "2", "3", "4"},
		{"", strings.Repeat("123", 3)},
	} {
		arr.Append(v)
	}
	input := []proto.InputColumn{
		{
			Name: "foo",
			Data: arr,
		},
	}
	block := &proto.Block{
		Info:    proto.BlockInfo{BucketNum: -1},
		Columns: 1,
		Rows:    3,
	}

	block.EncodeAware(b, proto.Version)
	for _, col := range input {
		col.EncodeStart(b)
		col.Data.EncodeColumn(b)
	}

	return b.Buf
}

func encodeTestIPv6Block() []byte {
	b := &proto.Buffer{}

	arr := proto.NewArrIPv6()
	for _, v := range [][]string{
		{"100::", "200::"},
		{"300::", "400::", "500::", "600::"},
		{"2001:db8::", "2002::"},
	} {
		var values []proto.IPv6
		for _, s := range v {
			ip := netip.MustParseAddr(s)
			values = append(values, proto.ToIPv6(ip))
		}
		arr.Append(values)
	}
	input := []proto.InputColumn{
		{
			Name: "foo",
			Data: arr,
		},
	}
	block := &proto.Block{
		Info:    proto.BlockInfo{BucketNum: -1},
		Columns: 1,
		Rows:    3,
	}

	block.EncodeAware(b, proto.Version)
	for _, col := range input {
		col.EncodeStart(b)
		col.Data.EncodeColumn(b)
	}

	return b.Buf
}

func TestEncodeIPv6Block(t *testing.T) {
	data := encodeTestIPv6Block()
	gold.Bytes(t, data, "test_arr_ipv6_block")

	r := proto.NewReader(bytes.NewReader(data))
	v := proto.Version
	a := proto.NewArrIPv6()
	d := proto.Results{
		{
			Name: "foo",
			Data: a,
		},
	}

	// Skip table name.
	if _, err := r.Str(); err != nil {
		t.Fatal(err)
	}

	var block proto.Block
	if err := block.DecodeBlock(r, v, d); err != nil {
		t.Fatal(err)
	}
	if block.End() {
		return
	}
}

func TestEncodeBlock(t *testing.T) {
	data := encodeTestStrBlock()
	gold.Bytes(t, data, "test_arr_str_block")

	r := proto.NewReader(bytes.NewReader(data))
	v := proto.Version
	d := proto.Results{
		{
			Name: "foo",
			Data: new(proto.ColStr).Array(),
		},
	}

	// Skip table name.
	if _, err := r.Str(); err != nil {
		t.Fatal(err)
	}

	var block proto.Block
	if err := block.DecodeBlock(r, v, d); err != nil {
		t.Fatal(err)
	}
	if block.End() {
		return
	}
}

func FuzzDecodeBlock(f *testing.F) {
	f.Add(encodeTestStrBlock())

	f.Fuzz(func(t *testing.T, data []byte) {
		r := proto.NewReader(bytes.NewReader(data))
		v := proto.Version
		d := proto.Results{
			{
				Name: "foo",
				Data: new(proto.ColStr).Array(),
			},
		}

		// Skip table name.
		if _, err := r.Str(); err != nil {
			t.Skip(err)
		}

		var block proto.Block
		if err := block.DecodeBlock(r, v, d); err != nil {
			t.Skip(err)
		}
	})
}

func makeArr[T any](v proto.ColumnOf[T], data [][]T) *proto.ColArr[T] {
	a := proto.NewArray(v)
	for _, s := range data {
		a.Append(s)
	}
	return a
}

func FuzzDecodeBlockAuto(f *testing.F) {
	addCorpus(f, []proto.ColInput{
		proto.ColInt8{1, 2, 3, 4, 5},
		make(proto.ColUInt256, 10),
		makeArr[string](new(proto.ColStr), [][]string{
			{"foo", "bar", "baz"},
			{"1000", "20000", "3000", "40000", "5000", "6000", "abc"},
			{"foo", "bar"},
			{"1"},
			{},
			{"1", "2", strings.Repeat("abc", 60)},
		}),
		makeArr[int8](new(proto.ColInt8), [][]int8{
			{1, 2, 3},
			make([]int8, 100),
			make([]int8, 1024),
			make([]int8, 2058),
			{},
			{100},
		}),
		proto.ColDateTime64Auto{
			Precision: 9,
			ColDateTime64: proto.ColDateTime64{
				1, 2, 3,
			},
		},
		makeArr[string](new(proto.ColStr).LowCardinality(), [][]string{
			{"foo", "bar", "baz"},
			{"1000", "20000", "3000", "40000", "5000", "6000", "abc"},
			{"foo", "bar"},
			{"1"},
			{},
			{"1", "2", strings.Repeat("abc", 60)},
		}),
	})

	f.Fuzz(func(t *testing.T, data []byte) {
		r := proto.NewReader(bytes.NewReader(data))
		v := proto.Version
		d := new(proto.Results).Auto()

		// Skip table name.
		if _, err := r.Str(); err != nil {
			t.Skip(err)
		}

		var block proto.Block
		if err := block.DecodeBlock(r, v, d); err != nil {
			t.Skip(err)
		}
	})
}

func addCorpus(f *testing.F, data []proto.ColInput) {
	for _, v := range data {
		b := &proto.Buffer{}
		input := []proto.InputColumn{
			{
				Name: "foo",
				Data: v,
			},
		}
		block := &proto.Block{
			Info:    proto.BlockInfo{BucketNum: -1},
			Columns: 1,
			Rows:    v.Rows(),
		}
		block.EncodeAware(b, proto.Version)
		for _, col := range input {
			col.EncodeStart(b)
			col.Data.EncodeColumn(b)
		}
		f.Add(b.Buf)
	}
}

func FuzzDecodeArrayIPv6ArrayBlock(f *testing.F) {
	f.Add(encodeTestStrBlock())

	f.Fuzz(func(t *testing.T, data []byte) {
		r := proto.NewReader(bytes.NewReader(data))
		v := proto.Version
		d := proto.Results{
			{
				Name: "foo",
				Data: proto.NewArrIPv6(),
			},
		}

		// Skip table name.
		if _, err := r.Str(); err != nil {
			t.Skip(err)
		}

		var block proto.Block
		if err := block.DecodeBlock(r, v, d); err != nil {
			t.Skip(err)
		}
	})
}
