package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"strings"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/segmentio/parquet-go"
	"google.golang.org/protobuf/proto"

	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/polarsignals/frostdb"
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
type Labels []LabelColumn

type LabelColumn struct {
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
func visualize() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: parquet-tool <file>")
		os.Exit(1)
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer f.Close()
	stats, err := f.Stat()
	if err != nil {
		panic(err)
	}
	pf, err := parquet.OpenFile(f, stats.Size())
	if err != nil {
		panic(err)
	}
	fmt.Println("schema:", pf.Schema())
	meta := pf.Metadata()
	fmt.Println("Num Rows:", meta.NumRows)

	for i, rg := range meta.RowGroups {
		fmt.Println("\t Row group:", i)
		fmt.Println("\t\t Row Count:", rg.NumRows)
		fmt.Println("\t\t Row size:", humanize.Bytes(uint64(rg.TotalByteSize)))
		fmt.Println("\t\t Columns:")
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Col", "Type", "NumVal", "Encoding", "TotalCompressedSize", "TotalUncompressedSize", "Compression", "%"})
		for _, ds := range rg.Columns {
			table.Append(
				[]string{
					strings.Join(ds.MetaData.PathInSchema, "/"),
					ds.MetaData.Type.String(),
					fmt.Sprintf("%d", ds.MetaData.NumValues),
					fmt.Sprintf("%s", ds.MetaData.Encoding),
					humanize.Bytes(uint64(ds.MetaData.TotalCompressedSize)),
					humanize.Bytes(uint64(ds.MetaData.TotalUncompressedSize)),
					fmt.Sprintf("%.2f", float64(ds.MetaData.TotalUncompressedSize-ds.MetaData.TotalCompressedSize)/float64(ds.MetaData.TotalCompressedSize)*100),
					fmt.Sprintf("%.2f", float64(ds.MetaData.TotalCompressedSize)/float64(rg.TotalByteSize)*100),
				})
		}
		table.Render()
	}
}
func simpleSchema() proto.Message {
	return &schemapb.Schema{
		Name: "simple_schema",
		Columns: []*schemapb.Column{{
			Name: "value",
			StorageLayout: &schemapb.StorageLayout{
				Type:        schemapb.StorageLayout_TYPE_DOUBLE,
				Encoding:    schemapb.StorageLayout_ENCODING_PLAIN_UNSPECIFIED,
				Compression: schemapb.StorageLayout_COMPRESSION_SNAPPY,
			},
			Dynamic: false,
		}, {
			Name: "time",
			StorageLayout: &schemapb.StorageLayout{
				Type:        schemapb.StorageLayout_TYPE_INT64,
				Encoding:    schemapb.StorageLayout_ENCODING_DELTA_BINARY_PACKED,
				Compression: schemapb.StorageLayout_COMPRESSION_SNAPPY,
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

//	func convertBlockFrostDB_single_push(path, blockID string) error {
//		tsdb, block, err := openBlock(path, blockID)
//		if err != nil {
//			return err
//		}
//		defer func() {
//			tsdb_errors.NewMulti(err, tsdb.Close()).Err()
//		}()
//
//		ir, err := block.Index()
//		if err != nil {
//			return err
//		}
//		defer ir.Close()
//
//		postingsr, err := ir.Postings(index.AllPostingsKey())
//		if err != nil {
//			return err
//		}
//		chunkr, err := block.Chunks()
//		if err != nil {
//			return err
//		}
//		defer func() {
//			err = tsdb_errors.NewMulti(err, chunkr.Close()).Err()
//		}()
//
//		// FrostDB
//
//		bucket, err := filesystem.NewBucket("data-promtool")
//		if err != nil {
//			return err
//		}
//		store, err := frostdb.New(
//			frostdb.WithBucketStorage(bucket),
//		)
//		if err != nil {
//			return err
//		}
//		db, err := store.DB(context.Background(), "prometheus")
//		if err != nil {
//			return err
//		}
//		tableSchema := simpleSchema()
//
//		table, err := db.Table(
//			"tsdb_table",
//			frostdb.NewTableConfig(tableSchema),
//		)
//		if err != nil {
//			return err
//		}
//		ctx := context.Background()
//		as, err := pqarrow.ParquetSchemaToArrowSchema(ctx, table.Schema().ParquetSchema(), logicalplan.IterOptions{})
//		chks := []chunks.Meta{}
//		builder := labels.ScratchBuilder{}
//
//		labelNamesMap := map[string]struct{}{}
//		for postingsr.Next() {
//			if err := ir.Series(postingsr.At(), &builder, &chks); err != nil {
//				return err
//			}
//			for name := range builder.Labels().Map() {
//				labelNamesMap[name] = struct{}{}
//			}
//		}
//		if postingsr.Err() != nil {
//			return postingsr.Err()
//		}
//
//		labelNames := make([]string, 0, len(labelNamesMap))
//		for name := range labelNamesMap {
//			labelNames = append(labelNames, name)
//		}
//		sort.Strings(labelNames)
//		mem := memory.NewGoAllocator()
//
//		rb := array.NewRecordBuilder(mem, as)
//
//		// Reset the postings reader by creating a new one. Seek doesn't work.
//		postingsr, err = ir.Postings(index.AllPostingsKey())
//		if err != nil {
//			return err
//		}
//		var it chunkenc.Iterator
//		for postingsr.Next() {
//			if err := ir.Series(postingsr.At(), &builder, &chks); err != nil {
//				return err
//			}
//
//			lset := builder.Labels()
//
//			for _, chk := range chks {
//				chk, err := chunkr.Chunk(chk)
//				if err != nil {
//					return err
//				}
//
//				it = chk.Iterator(it)
//				for it.Next() == chunkenc.ValFloat {
//					t, v := it.At()
//					rb.Append(lset, t, v)
//				}
//			}
//		}
//
//		r := rb.NewRecord()
//		defer r.Release()
//
//		_, err = table.InsertRecord(context.Background(), r)
//		if err != nil {
//			return err
//		}
//
//		return store.Close()
//	}
func promMatchersToFrostDBExprs(matchers []*labels.Matcher) logicalplan.Expr {
	exprs := []logicalplan.Expr{}
	for _, matcher := range matchers {
		switch matcher.Type {
		case labels.MatchEqual:
			exprs = append(exprs, logicalplan.Col("labels."+matcher.Name).Eq(logicalplan.Literal(matcher.Value)))
		case labels.MatchNotEqual:
			exprs = append(exprs, logicalplan.Col("labels."+matcher.Name).NotEq(logicalplan.Literal(matcher.Value)))
		case labels.MatchRegexp:
			exprs = append(exprs, logicalplan.Col("labels."+matcher.Name).RegexMatch(matcher.Value))
		case labels.MatchNotRegexp:
			exprs = append(exprs, logicalplan.Col("labels."+matcher.Name).RegexNotMatch(matcher.Value))
		}
	}
	fmt.Println(exprs)
	return logicalplan.And(exprs...)
}
func readTsdb(path string, blockID string) error {
	db, _, err := openBlock(path, blockID)
	if err != nil {
		return err
	}
	defer db.Close()
	//bucket, err := filesystem.NewBucket(path)
	if err != nil {
		return err
	}
	defer func() {
		err = tsdb_errors.NewMulti(err, db.Close()).Err()
	}()
	q, err := db.Querier(context.Background(), math.MinInt64, math.MaxInt64)
	if err != nil {
		return err
	}
	defer q.Close()
	sset := q.Select(true, nil, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+"))
	ctx := context.Background()
	// Create a new column store
	columnstore, err := frostdb.New(
	//frost.WithWAL(),
	//frost.WithStoragePath(path),
	//frost.WithBucketStorage(bucket),
	)
	if err != nil {
		return err
	}
	defer columnstore.Close()

	// Open up a database in the column store
	database, err := columnstore.DB(ctx, "simple_db")
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
	pqLbls := make([]LabelColumn, 0, 10)
	rows := make([]any, 0, 1000)
	for sset.Next() {
		rows = rows[:0]
		pqLbls = pqLbls[:0]

		series := sset.At()
		lbs := series.Labels()
		lbs.Range(func(l labels.Label) {
			pqLbls = append(pqLbls, LabelColumn{Name: l.Name, Value: l.Value})
		})
		//fmt.Println(pqLbls)
		it := series.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			ts, val := it.At()
			rows = append(rows, Data{
				Value: val,
				Time:  ts,
				LABEL: pqLbls,
			})
		}
		//fmt.Println("Writing to table")
		_, err = table.Write(context.Background(), rows...)
		if err != nil {
			fmt.Println(err)
			return err
		}

		if it.Err() != nil {
			return sset.Err()
		}
	}
	fmt.Println("Write succesfull")
	//table.ActiveBlock().Persist()
	//
	// Create a new query engine to retrieve data and print the results
	engine := query.NewEngine(memory.DefaultAllocator, database.TableProvider())
	sets := map[string]struct{}{}
	start := math.MinInt64
	end := math.MaxInt64
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"), labels.MustNewMatcher(labels.MatchEqual, "instance", "localhost:9090"), labels.MustNewMatcher(labels.MatchEqual, "job", "prometheus")}
	err1 := engine.ScanTable("tsdb_table").
		Filter(logicalplan.And(
			logicalplan.And(
				logicalplan.Col("time").Gt(logicalplan.Literal(start)),
				logicalplan.Col("time").Lt(logicalplan.Literal(end)),
			),
			promMatchersToFrostDBExprs(matchers),
		)).
		Project(
			logicalplan.DynCol("labels"),
			logicalplan.Col("time"),
			logicalplan.Col("value"),
		).Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
		fmt.Println(r)
		defer r.Release()
		for i := 0; i < int(r.NumCols()); i++ {
			fmt.Println(r.ColumnName(i))
			sets[r.ColumnName(i)] = struct{}{}
		}
		return nil
	})
	if err1 != nil {
		fmt.Printf(" failed to perform labels query: %s", err1)
	}

	names := []string{}
	for s := range sets {
		names = append(names, strings.TrimPrefix(s, "labels."))
	}
	fmt.Println(names)
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
