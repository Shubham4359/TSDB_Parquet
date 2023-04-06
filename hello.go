package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"strings"

	"github.com/apache/arrow/go/arrow/memory"
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/polarsignals/frostdb"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"google.golang.org/protobuf/proto"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
)

type Data struct {
	Value float64
	Time  int64
	LABEL Labels
}
type Labels []Label

type Label struct {
	Name, Value string
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
func simpleSchema() proto.Message {
	return &schemapb.Schema{
		Name: "simple_schema",
		Columns: []*schemapb.Column{{
			Name: "value",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_DOUBLE,
			},
			Dynamic: false,
		}, {
			Name: "time",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
			Dynamic: false,
		}, {
			Name: "label",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
				Nullable: true,
			},
			Dynamic: true,
		}},
		SortingColumns: []*schemapb.SortingColumn{{
			Name:      "time",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}, {
			Name:       "label",
			NullsFirst: true,
			Direction:  schemapb.SortingColumn_DIRECTION_ASCENDING,
		}},
	}
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

	// Create a new column store
	columnstore, err := frostdb.New()
	if err != nil {
		return err
	}
	defer columnstore.Close()

	// Open up a database in the column store
	database, err := columnstore.DB(context.Background(), "simple_db")
	if err != nil {
		return err
	}
	// Define our simple schema of labels and values
	schema := simpleSchema()

	// Create a table named simple in our database
	table, err := database.Table(
		"tsdb_table",
		frostdb.NewTableConfig(schema),
	)
	if err != nil {
		return err
	}
	for sset.Next() {
		series := sset.At()
		lbs := series.Labels()
		l := make([]Label, 0)
		for _, v := range lbs {
			r1 := strings.Split(fmt.Sprint(v), " ")
			l = append(l, Label{Name: r1[0], Value: r1[1]})
		}
		//res, err := convertLabelsToString(m1)
		//fmt.Println(jsonstr)
		it := series.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			ts, val := it.At()
			d := Data{
				Value: val,
				Time:  ts,
				LABEL: l,
			}
			_, err = table.Write(context.Background(), d)
			if err != nil {
				fmt.Println(d)
				fmt.Println(err)
				return err
			}
		}
		if it.Err() != nil {
			return sset.Err()
		}
	}
	fmt.Println("Write succesfull")
	// Create a new query engine to retrieve data and print the results
	engine := query.NewEngine(memory.DefaultAllocator, database.TableProvider())
	_ = engine.ScanTable("tsdb_table").
		Project(logicalplan.DynCol("label")). // We don't know all dynamic columns at query time, but we want all of them to be returned.
		Filter(
			logicalplan.Col("label").Eq(logicalplan.Literal("job prometheus")),
		).Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
		fmt.Println(r)
		return nil
	})
	return nil
}
func main() {

	path, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	blockId := "01GW1T7K3E9F9R361GDPVH8NZF"
	err = readTsdb(path, blockId)
	if err == nil {
		fmt.Println("Write Successful")
	} else {
		fmt.Println("error")
		fmt.Println(err)
	}

}
