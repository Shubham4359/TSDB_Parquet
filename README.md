<div align="center">
<img src="assets/lfx-2021-1.svg" height= "auto" width="200" />
<br />
<img src="assets/lfx-2021-2.webp" height= "auto" width="200" />
<br />
<h1>moja global</h1>
<h3>
Cloud native measurement, reporting and validation of carbon emissions
</h3>
<a href="https://github.com/iamrajiv/lfx-2021/network/members"><img src="https://img.shields.io/github/forks/iamrajiv/lfx-2021?color=0969da&style=for-the-badge" height="auto" width="auto" /></a>
<a href="https://github.com/iamrajiv/lfx-2021/stargazers"><img src="https://img.shields.io/github/stars/iamrajiv/lfx-2021?color=0969da&style=for-the-badge" height="auto" width="auto" /></a>
<a href="https://github.com/iamrajiv/lfx-2021/blob/main/LICENSE"><img src="https://img.shields.io/github/license/iamrajiv/lfx-2021?color=0969da&style=for-the-badge" height="auto" width="auto" /></a>
</div>

# LFX Mentorship'23 @CNCF:Thanos
Link to issue :- https://github.com/thanos-io/promql-engine/issues/167

# Why are we doing this conversion
Columnar data stores have become incredibly popular for analytics. Structuring data in columns instead of rows leverages the architecture of modern hardware, allowing for efficient processing of data. A columnar data store might be right for you if you have workloads where you write a lot of data and need to perform analytics on that data.

# Thanos
Prometheus/Thanos, a Cloud Native Computing Foundation project, is a systems and service monitoring system. It collects metrics from configured targets at given intervals, evaluates rule expressions, displays the results, and can trigger alerts when specified conditions are observed.
One feature of thanos is multi-dimensional data model (time series defined by metric name and set of key/value dimensions).

# Understanding Problem
While columnar databases already exist, most require a static schema. However, Observability workloads differ in that data their schemas are not static, meaning not all columns are pre-defined. Wide column databases already exist, but typically are not strictly typed (e.g. document databases), and most wide-column databases are row-based databases, not columnar databases.

Take a Prometheus time-series for example. Prometheus time-series are uniquely identified by the combination of their label-sets:
               http_requests_total{path="/api/v1/users", code="200"} 12

This model does not map well into a static schema, as label-names cannot be known upfront. The most suitable data-type some columnar databases have to offer is a map, however, maps have the same problems as row-based databases, where all values of a map in a row are stored together, resulting in an inability to exploit the advantages of a columnar layout.
To resolve this Problem we have used FrostDB schema can define a column to be dynamic, causing a column to be created on the fly when a new label-name is seen.

```
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
```

With this schema, all rows are expected to have a `time` and a `value` but can vary in their columns prefixed with `label.`. In this schema all dynamically created columns are still Dictionary and run-length encoded and must be of type `string` as seen from type and encoding.
Here sorting is done on the basis of time stamp in ascending order.
Value and time column are not dynamic in nature as they are present in each series.

# Analysing Parquet Files

To analyse Parquet files being formed corresponding to the TSDB block we have to use persistance for analysis.
Given that you know the path of TSDB block
You can create a frostdb table using below snippet.
```
        bucket, err := filesystem.NewBucket(path)
	ctx := context.Background()
	// Create a new column store
	columnstore, err := frostdb.New(
	frost.WithWAL(),
	frost.WithStoragePath(path),
	frost.WithBucketStorage(bucket),
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

	// Create a table named tsdb_table in our database
	table, err := database.Table(
		"tsdb_table",
		frostdb.NewTableConfig(schema),
	)
	if err != nil {
		return err
	}
```
Now according to schema append the values in table.
Once done with that persist the table and you will have a output parquet file which you can then analyse.
```
table.ActiveBlock().Persist()
```
Now to analyse the parquet file we can run the below binary and point our parquet file to that.
https://github.com/polarsignals/frostdb/blob/main/cmd/parquet-tool/main.go

```
go run cmd/parquet-tool/main.go <path-parquet-file>
```
Post this you can analyse the parquet files properties like Size, labels, encoding, compression,etc.

# Querier
Currently still working on making querier generic.
Here we can set some labels and provide a start and end time between that all timeseries data will be given as output.
Also we can pick any matchers according to our need.
Example of Labels can look like
```
matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"), labels.MustNewMatcher(labels.MatchEqual, "instance", "localhost:9090"), labels.MustNewMatcher(labels.MatchEqual, "job", "prometheus")}
```
Different Matchers Considered are 
```
 * MatchEqual
 * MatchNotEqual
 * MatchRegexp  
 * MatchNotRegexp
```

Post this choose the Start and End time and run querier function
Important to note that while taking projections make Label as Dynamic Column and Rest two columns(time and value) as normal column.

# Running 
To Run this repositry we can use
```
go run hello.go
```

Note:- Currently I have commented the code for persistance and visualisation as working upon making the querier genric.
