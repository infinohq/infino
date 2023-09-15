# Benchmark - Elasticsearch, Clickhouse and Tantivy Comparison with Infino

This pacakge contains comparision of Infino with [Elasticsearch](https://github.com/elastic/elasticsearch-rs) and [Tantivy](https://github.com/quickwit-oss/tantivy). The raw output of comparison can be found [here](output.txt).

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
  - Unzip and modify the prometheus.yaml file as below
  - add scrape_config
    ```
    - job_name: 'rust_app'
    static_configs:
      - targets: ['localhost:9000']
    ```
  - Change scrape interval to 1s `scrape_interval: 1s`
  - Start prometheus simply by running the binary `./prometheus`
- Install [Clickhouse](https://clickhouse.com/docs/en/install)
  - Create directory `benches/ch-tmp` and move clickhouse binary there, srtart server from this directory using `./clickhouse server`
  - Create database `test_logs` using clickhouse client using command `create database test_logs`
- Start Infino server
  - Run `make run` from `infino` directory

- Run benchmark

```
$ cd benches
$ cargo run -r
```

## Results

The below tests were executed on MacBook Pro (16-inch, 2023) having Apple M2 Max chipset and 32Gb of Ram

### Index size

| dataset                      | Elasticsearch   | Tantivy         | Clickhouse       | Infino          | Infino-Rest     |
| ---------------------------- | --------------- | --------------- | -----------------| --------------- | --------------- |
| Apache Log (5,135,877 bytes) | 2,411,725 bytes | 3,146,858 bytes | 27,042,683 bytes | 1,848,698 bytes | Same as Infino  |

### Insertion speed

| dataset    | Elasticsearch | Tantivy    | Clickhouse | Infino   | Infino-Rest |
| ---------- | ------------- | ---------- | ---------- | -------- | ----------- |
| Apache Log | 7,180 ms      | 422.71 ms  | 728.58 ms  | 267.23ms | 661.69ms    |

### Search latency

| # of words in query | Elasticsearch | Tantivy   | Clickhouse | Infino    | Infino-Rest |
| ------------------- | ------------- | --------- | ---------- | --------- | ----------- |
| 1                   | 50ms          | 1.45ms    | 6.60ms     | 1.03ms    | 942.42µs    |
| 2                   | 4ms           | 61.25µs   | 7.23ms     | 12.25µs   | 710.88µs    |
| 3                   | 49ms          | 2.32ms    | 10.63ms    | 106.32ms  | 418.25µs    |
| 4                   | 38ms          | 3.30ms    | 8.05ms     | 29.97ms   | 399.17µs    |
| 5                   | 34ms          | 155.88µs  | 7.39ms     | 185.83µs  | 531.75µs    |
| 6                   | 37ms          | 111.29µs  | 8.02ms     | 1.57ms    | 599.13µs    |
| 7                   | 31ms          | 4.34mms   | 9.28ms     | 86.46ms.  | 577.21µs    |

### Timeseries search latency

Average over 10 queries made

| Data points | Prometheus | Infino     |
| ----------- | ---------- | ---------- |
| CPU Usage   |    2035µs  |   1193µs   |
