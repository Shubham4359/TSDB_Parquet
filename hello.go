package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"

	parquet1 "github.com/segmentio/parquet-go"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
)

type TSDB struct {
	Value float64 `parquet:"name=value"`
	Time  int64   `parquet:"name=time"`
	Label string  `parquet:"name=label"`
}

func SlicesEqual(a, b []*TSDB) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		} else {
			fmt.Println(v, b[i])
		}
	}
	return true
}
func generateParquet(block []*TSDB) error {
	// using xitongsys library
	log.Println("generating parquet file")
	fw, err := local.NewLocalFileWriter("output.parquet")
	fmt.Println(len(block))
	if err != nil {
		return err
	}
	//parameters: writer, type of struct, size
	pw, err := writer.NewParquetWriter(fw, new(TSDB), int64(len(block)))
	if err != nil {
		return err
	}
	//compression type
	pw.CompressionType = parquet.CompressionCodec_GZIP
	defer fw.Close()
	for _, d := range block {
		if err = pw.Write(d); err != nil {
			return err
		}
	}
	if err = pw.WriteStop(); err != nil {
		return err
	}
	u, err := readParquet(int64(len(block)))
	for _, vv := range u {
		fmt.Println(vv)
	}
	if err != nil {
		return err
	} else {
		if SlicesEqual(u, block) {
			fmt.Println("Writing done succesfully both same")
		} else {
			fmt.Println("Blocks unequal")
		}
	}
	return nil
}
func readParquet(recordNumber int64) ([]*TSDB, error) {
	fr, err := local.NewLocalFileReader("output.parquet")
	if err != nil {
		return nil, err
	}
	pr, err := reader.NewParquetReader(fr, new(TSDB), recordNumber)
	if err != nil {
		return nil, err
	}
	u := make([]*TSDB, recordNumber)
	if err = pr.Read(&u); err != nil {
		return nil, err
	}
	pr.ReadStop()
	fr.Close()
	return u, nil
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
	var block []*TSDB
	writer := parquet1.NewWriter(file)

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
			block = append(block, &tsdb)
			//fmt.Println(tsdb)
			if err := writer.Write(tsdb); err != nil {
				return err
			}
		}
		if it.Err() != nil {
			return sset.Err()
		}
	}
	generateParquet(block)
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
