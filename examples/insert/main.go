package main

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func main() {
	ctx := context.Background()
	c, err := ch.Dial(ctx, ch.Options{Address: "localhost:9000"})
	if err != nil {
		panic(err)
	}

	var ColumnTypeDDSketch proto.ColumnType = "AggregateFunction(quantilesDDSketch(0.001, 0.9), UInt64)"
	data := proto.NewAggregateFunctionDDSketch(ColumnTypeDDSketch)

	if err := c.Do(ctx, ch.Query{
		Body: "SELECT sketch FROM default.02919_ddsketch_quantile",
		Result: proto.Results{
			{Name: "sketch", Data: data},
		},
		// OnResult will be called on next received data block.
		OnResult: func(ctx context.Context, b proto.Block) error {
			fmt.Println("OnResult", data.Rows())
			fmt.Println(data.Debug())
			return nil
		},
	}); err != nil {
		panic(err)
	}
}
