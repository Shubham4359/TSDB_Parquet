package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/segmentio/parquet-go"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
)

var wg sync.WaitGroup

type TSDB struct {
	Value float64 `parquet:"value"`
	Time  int64   `parquet:"time"`
	Label string  `parquet:"label"`
}

func openBlock(path, blockID string) (*tsdb.DBReadOnly, tsdb.BlockReader, error) {
	db, err := tsdb.OpenDBReadOnly(path, nil)
	if err != nil {
		fmt.Println("error at p1")
		return nil, nil, err
	}
	blocks, err := db.Blocks()
	if err != nil {

		fmt.Println("error at p2")
		return nil, nil, err
	}
	var block tsdb.BlockReader
	if blockID != "" {
		for _, b := range blocks {
			if b.Meta().ULID.String() == blockID {
				block = b
				break
			}
		}
	} else if len(blocks) > 0 {
		block = blocks[len(blocks)-1]
	}
	if block == nil {

		fmt.Println("error at p3")
		return nil, nil, fmt.Errorf("block %s not found", blockID)
	}
	return db, block, nil
}
func readTsdb(path string, blockID string) error {
	db, _, err := openBlock(path, blockID)
	if err != nil {

		fmt.Println("error at p4")
		return err
	}
	defer func() {
		err = tsdb_errors.NewMulti(err, db.Close()).Err()
	}()
	q, err := db.Querier(context.Background(), math.MinInt64, math.MaxInt64)
	if err != nil {

		fmt.Println("error at p5")
		return err
	}
	defer q.Close()

	sset := q.Select(false, nil, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+"))

	file, err := os.Create("../output_tsdb.parquet")
	if err != nil {

		fmt.Println("error at p6")
		return err
	}

	writer := parquet.NewWriter(file)

	for sset.Next() {
		series := sset.At()
		lbs := series.Labels().String()
		it := series.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			ts, val := it.At()
			tsdb := TSDB{
				Value: val,
				Time:  ts,
				Label: lbs,
			}
			//fmt.Println(tsdb)
			if err := writer.Write(tsdb); err != nil {

				fmt.Println("error at p7")
				return err
			}
		}
		if it.Err() != nil {
			fmt.Println("error at p77")
			return sset.Err()
		}
	}
	defer db.Close()

	return nil
}
func main() {

	//fname := "./hello.go"
	//path, err := filepath.Abs(fname)
	path, err := os.Getwd()
	fmt.Println(path)
	if err != nil {
		fmt.Println("error at p9")
	}

	blockId := "01GW1T7K3E9F9R361GDPVH8NZF"
	//p := filepath.join(path, blockId)
	//p := filepath.Join(path, blockId)
	err = readTsdb(path, blockId)
	wg.Wait()
	if err == nil {
		fmt.Println("Write Successful")
	} else {
		fmt.Println(err)
	}
}
