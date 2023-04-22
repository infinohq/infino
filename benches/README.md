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
| Apache Log | 2400000 bytes | 3207319 bytes | 1832848 bytes |

### Insertion speed

| dataset    | Elasticsearch        | Tantivy | Infino   |
| ---------- | -------------------- | ------- | -------- |
| Apache Log | 3.68s (Over network) | 1.95s   | 315.23ms |

### Search latency

Average across 5 runs for Apache log dataset

| # of words in query | Elasticsearch | Tantivy   | Infino    |
| ------------------- | ------------- | --------- | --------- |
| 1                   | 169 ms        | 2.85ms.   | 1.48ms.   |
| 2                   | 7 ms          | 88.29µs.  | 17.75µs.  |
| 3                   | 145 ms        | 2.54ms.   | 144.71ms. |
| 4                   | 155 ms        | 3.99ms.   | 37.33ms.  |
| 5                   | 135 ms        | 156.79µs. | 234.08µs. |
| 6                   | 127 ms        | 115.58µs. | 2.15ms.   |
| 7                   | 160 ms        | 4.82ms.   | 115.70ms. |
