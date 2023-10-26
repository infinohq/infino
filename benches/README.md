# Benchmark - Elasticsearch, Clickhouse and Tantivy Comparison with Infino

This pacakge contains comparision of Infino with [Elasticsearch](https://github.com/elastic/elasticsearch-rs), [Clickhouse](https://github.com/ClickHouse/ClickHouse) and [Tantivy](https://github.com/quickwit-oss/tantivy). The raw output of comparison can be found [here](output.txt).

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

## Results (Apache log - small):

Run date: 2023-10-23

Operating System: macos

Machine description: Macbook Pro 16 inch, M1, 16GB RAM

Dataset: data/Apache.log

Dataset size: 5135877 bytes

### Index size

| dataset         | Elasticsearch | Clickhouse     | Infino        | Infino-Rest    |
| --------------- | ------------- | -------------- | ------------- | -------------- |
| data/Apache.log | 2411725 bytes | 35488638 bytes | 1097279 bytes | Same as infino |

### Indexing latency

| dataset         | Elasticsearch         | Clickhouse           | Infino              | Infino-Rest          |
| --------------- | --------------------- | -------------------- | ------------------- | -------------------- |
| data/Apache.log | 35378212 microseconds | 1364551 microseconds | 427316 microseconds | 1081403 microseconds |

### Search latency

Average across different query types. See the detailed output for granular info.

| dataset         | Elasticsearch       | Clickhouse         | Infino           | Infino-Rest       |
| --------------- | ------------------- | ------------------ | ---------------- | ----------------- |
| data/Apache.log | 134571 microseconds | 18084 microseconds | 888 microseconds | 3804 microseconds |

### Timeseries search latency

Average over 10 queries on time series.

| Data points    | Prometheus        | Infino            |
| -------------- | ----------------- | ----------------- |
| Search Latency | 2678 nanoseconds | 2864 nanoseconds |

## Results (Apache log - medium): 

Run date: 2023-10-26

Operating System: macos

Machine description: Macbook Pro 2023, 16 inch, M2 Max, 64 GB RAM

Dataset: data/Apache.log

Dataset size: 104,857,667 bytes



### Index size

| dataset | Elasticsearch | Clickhouse | Infino | Infino-Rest |
| ----- | ----- | ----- | ----- | ---- |
| data/Apache.log | 46,241,201 bytes | 822,056,628 bytes | 34,354,516 bytes | Same as infino |


### Indexing latency

| dataset | Elasticsearch | Clickhouse | Infino | Infino-Rest |
| ----- | ----- | ----- | ----- | ---- |
| data/Apache.log | 141,224,285 microseconds  | 16,924,729 microseconds  | 29,839,987 microseconds  | 44,180,081 microseconds  |


### Search latency

Average across different query types. See the detailed output for granular info.

| dataset | Elasticsearch | Clickhouse | Infino | Infino-Rest |
| ----- | ----- | ----- | ---- | ---- |
| data/Apache.log | 124,285 microseconds  | 34,469 microseconds  | 12,648 microseconds  | 32,689 microseconds  |


### Timeseries search latency

Average over 10 queries on time series.

| Data points | Prometheus | Infino |
| ----------- | ---------- | ---------- |
| Search Latency | 1,770 microseconds | 459 microseconds |