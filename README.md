# LFX Mentorship - Thanos
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
