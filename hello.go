package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"

	"github.com/segmentio/parquet-go"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
)

type TSDB struct {
	Value float64 `parquet:"value"`
	Time  int64   `parquet:"time"`
	Label string  `parquet:"label"`
}

func openBlock(path, blockID string) (*tsdb.DBReadOnly, tsdb.BlockReader, error) {
	db, err := tsdb.OpenDBReadOnly(path, nil)
	if err != nil {
		return nil, nil, err
	}
	blocks, err := db.Blocks()
	if err != nil {
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
		return nil, nil, fmt.Errorf("block %s not found", blockID)
	}
	return db, block, nil
}
func readTsdb(path string, blockID string) error {
	db, _, err := openBlock(path, blockID)
	if err != nil {
		return err
	}
	defer db.Close()
	defer func() {
		err = tsdb_errors.NewMulti(err, db.Close()).Err()
	}()
	q, err := db.Querier(context.Background(), math.MinInt64, math.MaxInt64)
	if err != nil {
		return err
	}
	defer q.Close()

	sset := q.Select(false, nil, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+"))

	file, err := os.Create("../output_tsdb.parquet")
	if err != nil {
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
				return err
			}
		}
		if it.Err() != nil {
			return sset.Err()
		}
	}
	return nil
}
func main() {

	//fname := "./hello.go"
	//path, err := filepath.Abs(fname)
	path, err := os.Getwd()
	//fmt.Println(path)
	if err != nil {
		log.Fatal(err)
	}

	blockId := "01GW1T7K3E9F9R361GDPVH8NZF"
	//p := filepath.join(path, blockId)
	//p := filepath.Join(path, blockId)
	err = readTsdb(path, blockId)
	if err == nil {
		fmt.Println("Write Successful")
	} else {
		fmt.Println(err)
	}
}
