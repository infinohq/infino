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

## Results:

Run date: 2023-10-19

Operating System: macos

Machine description: Macbook Pro 16 inch, M1, 16GB RAM

Dataset: data/Apache.log

Dataset size: 5135877 bytes

### Index size

| dataset         | Elasticsearch | Clickhouse     | Infino        | Infino-Rest    |
| --------------- | ------------- | -------------- | ------------- | -------------- |
| data/Apache.log | 2411725 bytes | 29477047 bytes | 1850754 bytes | Same as infino |

### Indexing latency

| dataset         | Elasticsearch        | Clickhouse          | Infino              | Infino-Rest         |
| --------------- | -------------------- | ------------------- | ------------------- | ------------------- |
| data/Apache.log | 7165613 microseconds | 713539 microseconds | 337953 microseconds | 778329 microseconds |

### Search latency

Average across different query types. See the detailed output for granular info.

| dataset         | Elasticsearch      | Clickhouse        | Infino           | Infino-Rest       |
| --------------- | ------------------ | ----------------- | ---------------- | ----------------- |
| data/Apache.log | 33571 microseconds | 8139 microseconds | 923 microseconds | 5743 microseconds |

### Timeseries search latency

Average over 10 queries on time series.

| Data points    | Prometheus        | Infino           |
| -------------- | ----------------- | ---------------- |
| Search Latency | 1796 microseconds | 602 microseconds |
