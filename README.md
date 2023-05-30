# LFX Mentorship - Thanos

Link to issue :- https://github.com/thanos-io/promql-engine/issues/167

# Why are we doing this conversion

Columnar data stores have become incredibly popular for analytics. Structuring data in columns instead of rows leverages the architecture of modern hardware, allowing for efficient processing of data. A columnar data store might be right for you if you have workloads where you write a lot of data and need to perform analytics on that data.

# Thanos
Prometheus/Thanos, a Cloud Native Computing Foundation project, is a systems and service monitoring system. It collects metrics from configured targets at given intervals, evaluates rule expressions, displays the results, and can trigger alerts when specified conditions are observed.
One feature of thanos is multi-dimensional data model (time series defined by metric name and set of key/value dimensions).
