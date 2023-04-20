# Benchmark - Elasticsearch and Tantivy Comparison with Infino

This pacakge contains comparision of Infino with [Elasticsearch](https://github.com/elastic/elasticsearch-rs) and [Tantivy](https://github.com/quickwit-oss/tantivy)

## Datasets

### Apache_2k.log

Apache logs, with thanks from the Logpai project - https://github.com/logpai/loghub

File is present in data folder named Apache.log

## Setup

- Install Elasticsearch by running following commands
  - `$ wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.6.0-linux-x86_64.tar.gz`
  - `$ tar xvfz elasticsearch-8.6.0-linux-x86_64.tar.gz`
  - Set `xpack.security.enabled` to `false` in `config/elasticsearch.yml`.
  - Start elasticsearch:
    - `$ bin/elasticsearch`
- Run benchmark

```
$ cd benches
$ cargo run -r
```

## Results

### Index size

| dataset    | Elasticsearch | Tantivy       | Infino        |
| ---------- | ------------- | ------------- | ------------- |
| Apache Log | 3100000 bytes | 3207319 bytes | 1832848 bytes |

### Insertion speed

| dataset    | Elasticsearch | Tantivy | Infino   |
| ---------- | ------------- | ------- | -------- |
| Apache Log | WIP           | 1.98s   | 313.64ms |

### Search latency

Average across 5 runs for Apache log dataset

| # of words in query | Elasticsearch | Tantivy  | Infino   |
| ------------------- | ------------- | -------- | -------- |
| 1                   | 54ms          | 277.17µs | 613.13µs |
| 2                   | WIP           | 10.87ms  | 3.85ms   |
| 3                   | WIP           | 4.79ms   | 648.21µs |
| 4                   | WIP           | 37.58ms  | 5.35ms   |
| 5                   | WIP           | 54.77ms  | 177.79µs |
