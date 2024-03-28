# Benchmark - ElasticSearch, Clickhouse and Prometheus performance comparison with Infino

Benchmarks are always hard but are important to provide a reasonable sense of performance. 

This package contains a comparision of Infino, Infino with OpenSearch, OpenSearch, Elasticsearch](https://github.com/elastic/elasticsearch-rs), [Prometheus](https://github.com/prometheus/prometheus), and [Clickhouse](https://github.com/ClickHouse/ClickHouse), the most popular OSS storage tools for observability today. The raw output of comparison can be found [here](output.txt). Feedback is welcome; we will add more tests as we go.

Want to jump directly to the results? Scroll below towards the end of this page.

## Datasets

### Apache.log

Apache logs, with thanks from the Logpai project - https://github.com/logpai/loghub

File is present in `benches/data` folder named Apache.log

## Setup

- Make sure to start from a clean slate (i.e., no other data indexed in any of the systems being compared),
  so that the results would be comparable.
- Make the Infino docker image
  - `make docker-build`
- Run benchmark

```
$ cd benches
$ cargo run -r
```

## Run only specific benchmarks

Sometimes, you may want to run only specific benchmarks, for example only Infino via REST API and Elastic, or only Infino via REST API. To do so you can use,
```
$ cargo run -r -- --infino-rest --elastic
```

## Run with different datasets
To use a different log file as input data use `--input-file` option as shown below.
```
$ cargo run -r -- --infino-rest --input-file=data/Apache_2G.log
```

## All run time option
Below are the options to run the benchmarks.
```
--infino
--infino-rest
--infino-os
--elastic
--clickhouse
--infino-metrics
--prometheus
--input-file=<path to input file>
```


## Results (Apache log - small):

Run date: 2023-10-23

Operating System: macos

Machine description: Macbook Pro 16 inch, M1, 16GB RAM

Dataset: data/Apache.log

Dataset size: 5.14 MB

### Log index size

| dataset         | Elasticsearch | Clickhouse     | Infino        | Infino-Rest    |
| --------------- | ------------- | -------------- | ------------- | -------------- |
| data/Apache.log | 2.41 MB | 35.49 MB | 1.10 MB | Same as infino |

### Log indexing latency

| dataset         | Elasticsearch         | Clickhouse           | Infino              | Infino-Rest          |
| --------------- | --------------------- | -------------------- | ------------------- | -------------------- |
| data/Apache.log | 35.38 s | 1.36s | 0.43 s | 1.08 s |

### Log search latency

Average across different query types. See the detailed output for granular info.

| dataset         | Elasticsearch       | Clickhouse         | Infino           | Infino-Rest       |
| --------------- | ------------------- | ------------------ | ---------------- | ----------------- |
| data/Apache.log | 134.57 ms | 18.08 ms | 0.888 ms | 3.80 ms |

### Metric search latency

Average over 10 queries on time series.

| Metric points  | Prometheus        | Infino            |
| -------------- | ----------------- | ----------------- |
| Search Latency | 2.68 μs  | 2.86 μs |

## Results (Apache log - medium): 

Run date: 2023-10-26

Operating System: macos

Machine description: Macbook Pro 2023, 16 inch, M2 Max, 64 GB RAM

Dataset: data/Apache.log

Dataset size: 104.86 MB



### Log index size

| dataset | Elasticsearch | Clickhouse | Infino | Infino-Rest |
| ----- | ----- | ----- | ----- | ---- |
| data/Apache.log | 46.24 MB | 822.06 MB | 34.35 MB | Same as infino |


### Log indexing latency

| dataset | Elasticsearch | Clickhouse | Infino | Infino-Rest |
| ----- | ----- | ----- | ----- | ---- |
| data/Apache.log | 141.22 s  | 16.92 s  | 29.84 s  | 44.18 s  |


### Log search latency

Average across different query types. See the detailed output for granular info.

| dataset | Elasticsearch | Clickhouse | Infino | Infino-Rest |
| ------- | ------------- | ---------- | ------ | ----------- |
| data/Apache.log | 124.29 ms  | 34.47 ms  | 12.65 ms  | 32.69 ms  |


### Metric search latency

Average over 10 queries on time series.

|  Metric points |     Prometheus     |      Infino      |
| -------------- | ------------------ | ---------------- |
| Search Latency | 1.77 ms | 0.46 ms |
