# Benchmark - ElasticSearch, Clickhouse and Prometheus performance comparison with Infino

Benchmarks are always hard but are important to provide a reasonable sense of performance. This package contains a comparision of Infino with [Elasticsearch](https://github.com/elastic/elasticsearch-rs), [Clickhouse](https://github.com/ClickHouse/ClickHouse) and [Prometheus](https://github.com/prometheus/prometheus), the most popular storage tools for observability today. The raw output of comparison can be found [here](output.txt). Feedback is welcome; we will add more tests as we go.

Want to jump directly to the results? Scroll below towards the end of this page.

## Datasets

### Apache.log

Apache logs, with thanks from the Logpai project - https://github.com/logpai/loghub

File is present in `benches/data` folder named Apache.log

## Setup

- Make sure to start from a clean slate (i.e., no other data indexed in any of the systems being compared),
  so that the results would be comparable.
- Install Elasticsearch by running following commands
  - `$ curl -O https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.6.0-linux-x86_64.tar.gz`
  - `$ tar xvfz elasticsearch-8.6.0-linux-x86_64.tar.gz`
  - Set `xpack.security.enabled` to `false` in `elasticsearch-8.6.0/config/elasticsearch.yml`
  - You may need to install JDK, set `ES_JAVA_HOME` environment variable, set `xpack.ml.enabled: false` depending on
    the hardware you are running on
  - Start elasticsearch:
    - `$ bin/elasticsearch`
- Install [prometheus](https://prometheus.io/download/) based on your architecure
  - Unzip and modify the prometheus.yml file as below
  - add scrape_config
    ```
    - job_name: 'rust_app'
    static_configs:
      - targets: ['localhost:9000']
    ```
  - Change scrape interval to 1s `scrape_interval: 1s`
  - Start prometheus simply by running the binary `./prometheus`
- Install [Clickhouse](https://clickhouse.com/docs/en/install)
  - Create directory `benches/ch-tmp` and move clickhouse binary there, start server from this directory using `./clickhouse server`
  - Drop database `test_logs` using clickhouse client, in case it exists already
  - Create database `test_logs` using clickhouse client using command `create database test_logs`
- Start Infino server

  - Make sure the `index` directory is empty - so that we are starting from a clean slate
  - Run `make run` from `infino` directory

- Run benchmark

```
$ cd benches
$ cargo run -r
```

## Run only Infino

Sometimes, you may want to run only Infino to see its resource usage, or profile the code. In this scenario, to run only Infino, use:
```
$ cargo run -r -- --stop-after-infino
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
| Search Latency | 2.68 us  | 2.86 us |

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