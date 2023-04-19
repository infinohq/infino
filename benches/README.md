# Benchmark - Elasticsearch and Tantivy Comparison with Infino

## Setup

### Elasticsearch

- Install elasticsearch:
  - `$ wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.6.0-linux-x86_64.tar.gz`
  - `$ tar xvfz elasticsearch-8.6.0-linux-x86_64.tar.gz`
- Set `xpack.security.enabled` to `false` in `config/elasticsearch.yml`.
- Start elasticsearch:
  - `$ bin/elasticsearch`

### Infino

- To run benchmark, cd into benches `cd benches` and run `cargo run`

## Results

### Elasticsearch

```
Elasticsearch index size in the following statement: [{"health":"green","status":"open","index":"perftest","uuid":"P9Ob6F4mTX6S3B6Osycs4g","pri":"1","rep":"0","docs.count":"56482","docs.deleted":"0","store.size":"3.1mb","pri.store.size":"3.1mb"}]
```

### Infino - Jan 2023 - no compression of inverted map, no support for blocks in postings list

Details of the infino index folder with size per serialized file:

```
Output of ls on infino index directory Output { status: ExitStatus(unix_wait_status(0)), stdout: "total 1.1M\n292K forward_map.bin\n584K inverted_map.bin\n4.0K metadata.bin\n164K terms.bin\n", stderr: "" }

Infino index size = 1059984 bytes
```

### Infino - Feb 5, 2023 - no compression of inverted map, block support in postings list

Posting list has support for blocks, but they are not yet compressed. Note that inverted index size increased from 584K to 604K.

```
Output of ls on infino index directory Output { status: ExitStatus(unix_wait_status(0)), stdout: "total 1.1M\n292K forward_map.bin\n604K inverted_map.bin\n4.0K metadata.bin\n164K terms.bin\n", stderr: "" }

Infino index size = 1080120 bytes
```

### Infino - Feb 6, 2023 - inverted map compressed

Posting list has support for blocks, and it is compressed. Note that inverted index size reduced from 604K to 568K. Likely the savings
will be higher for larger datasets with more keyword repetitions.

```
Output of ls on infino index directory Output { status: ExitStatus(unix_wait_status(0)), stdout: "total 1.1M\n292K forward_map.bin\n568K inverted_map.bin\n4.0K metadata.bin\n164K terms.bin\n", stderr: "" }

Infino index size = 1040242 bytes
```

### Infino - Feb 20, 2023 - many changes (esp using DashMap instead of HashMap)

Forward map size has increased quite a bit. Slight increase in terms as well. Inverted index increase is expected as last postings block is stored uncompressed.

```
Output of ls on infino index directory Output { status: ExitStatus(unix_wait_status(0)), stdout: "/tmp/infino-index-e1a892d1-db14-4f69-b06e-dcee82724777:\ntotal 12K\n4.0K 0\n4.0K all_segments_list.bin\n4.0K metadata.bin\n\n/tmp/infino-index-e1a892d1-db14-4f69-b06e-dcee82724777/0:\ntotal 1.8M\n928K forward_map.bin\n660K inverted_map.bin\n4.0K metadata.bin\n4.0K tags.bin\n188K terms.bin\n4.0K time_series.bin\n", stderr: "" }
Infino index size = 1813081 bytes
```

### Infino - Apr 2, 2023

Comparision with Tantivy index

Infino

```
Output of ls on infino index directory Output { status: ExitStatus(unix_wait_status(0)), stdout: "total 16\n0 0\n8 all_segments_list.bin\n8 metadata.bin\n\n/tmp/infino-index-b87bd31b-e12b-45a7-9b82-79c4e1445cae/0
:\ntotal 3560\n1760 forward_map.bin\n1360 inverted_map.bin\n   8 labels.bin\n   8 metadata.bin\n 416 terms.bin\n   8 time_series.bin\n", stderr: "" }
Infino index size = 1799524 bytes

Infino time required for insertion: 2.44s
```

Tantivy without STORED flag

```
Output of ls on tantivy index directory Output { status: ExitStatus(unix_wait_status(0)), stdout: "total 4768\n  8 40771ae9a0004963a292cfb657366ce8.fast\n 16 40771ae9a0004963a292cfb657366ce8.fieldnorm\n256 40771a
e9a0004963a292cfb657366ce8.idx\n128 40771ae9a0004963a292cfb657366ce8.pos\n 48 40771ae9a0004963a292cfb657366ce8.store\n 32 40771ae9a0004963a292cfb657366ce8.term\n  8 56ed5e64d71a4b86b28d8863f6b6747f.fast\n 16 56ed
5e64d71a4b86b28d8863f6b6747f.fieldnorm\n320 56ed5e64d71a4b86b28d8863f6b6747f.idx\n152 56ed5e64d71a4b86b28d8863f6b6747f.pos\n 64 56ed5e64d71a4b86b28d8863f6b6747f.store\n 32 56ed5e64d71a4b86b28d8863f6b6747f.term\n
 8 707528431d53434eb350b670c43c5556.fast\n 16 707528431d53434eb350b670c43c5556.fieldnorm\n304 707528431d53434eb350b670c43c5556.idx\n152 707528431d53434eb350b670c43c5556.pos\n 56 707528431d53434eb350b670c43c5556.s
tore\n 32 707528431d53434eb350b670c43c5556.term\n  8 7c31de0d8ecd4aa4ae34be988f6b7db6.fast\n 16 7c31de0d8ecd4aa4ae34be988f6b7db6.fieldnorm\n280 7c31de0d8ecd4aa4ae34be988f6b7db6.idx\n136 7c31de0d8ecd4aa4ae34be988f
6b7db6.pos\n 56 7c31de0d8ecd4aa4ae34be988f6b7db6.store\n 32 7c31de0d8ecd4aa4ae34be988f6b7db6.term\n  8 df099061a9e74e3281476e6d02ac2501.fast\n 16 df099061a9e74e3281476e6d02ac2501.fieldnorm\n320 df099061a9e74e3281
476e6d02ac2501.idx\n160 df099061a9e74e3281476e6d02ac2501.pos\n 64 df099061a9e74e3281476e6d02ac2501.store\n 32 df099061a9e74e3281476e6d02ac2501.term\n  8 e11979adf5ae40869f1a912394fbdb07.fast\n 16 e11979adf5ae4086
9f1a912394fbdb07.fieldnorm\n456 e11979adf5ae40869f1a912394fbdb07.idx\n160 e11979adf5ae40869f1a912394fbdb07.pos\n 64 e11979adf5ae40869f1a912394fbdb07.store\n 32 e11979adf5ae40869f1a912394fbdb07.term\n  8 e4336485c
fa9449fb844364b7fec6900.fast\n 16 e4336485cfa9449fb844364b7fec6900.fieldnorm\n456 e4336485cfa9449fb844364b7fec6900.idx\n168 e4336485cfa9449fb844364b7fec6900.pos\n 64 e4336485cfa9449fb844364b7fec6900.store\n 32 e4
336485cfa9449fb844364b7fec6900.term\n  8 e70e1df7795544e795fbd3779e7abac8.fast\n 16 e70e1df7795544e795fbd3779e7abac8.fieldnorm\n264 e70e1df7795544e795fbd3779e7abac8.idx\n128 e70e1df7795544e795fbd3779e7abac8.pos\n
 56 e70e1df7795544e795fbd3779e7abac8.store\n 32 e70e1df7795544e795fbd3779e7abac8.term\n  0 f753ddef082b42bb91d40f27cfa41574.fast\n  0 f753ddef082b42bb91d40f27cfa41574.store\n  8 meta.json\n", stderr: "" }
Tantivy index size without STORED flag = 2209855 bytes

Tantivy time required for insertion: 2.26s
```

Tantivy with STORED flag

```
Output of ls on tantivy index directory Output { status: ExitStatus(unix_wait_status(0)), stdout: "total 7120\n  8 0d79fe4d210046d89b00db58167b11f7.fast\n 16 0d79fe4d210046d89b00db58167b11f7.fieldnorm\n328 0d79fe
4d210046d89b00db58167b11f7.idx\n160 0d79fe4d210046d89b00db58167b11f7.pos\n392 0d79fe4d210046d89b00db58167b11f7.store\n 32 0d79fe4d210046d89b00db58167b11f7.term\n  8 199c7e3bd80b471187f10b0de106d129.fast\n 16 199c
7e3bd80b471187f10b0de106d129.fieldnorm\n256 199c7e3bd80b471187f10b0de106d129.idx\n128 199c7e3bd80b471187f10b0de106d129.pos\n272 199c7e3bd80b471187f10b0de106d129.store\n 32 199c7e3bd80b471187f10b0de106d129.term\n
 8 5e8891b7bf1a43548d8ab2bf485fd6d7.fast\n 24 5e8891b7bf1a43548d8ab2bf485fd6d7.fieldnorm\n448 5e8891b7bf1a43548d8ab2bf485fd6d7.idx\n176 5e8891b7bf1a43548d8ab2bf485fd6d7.pos\n400 5e8891b7bf1a43548d8ab2bf485fd6d7.s
tore\n 32 5e8891b7bf1a43548d8ab2bf485fd6d7.term\n  8 a193d6ef736e47faad2aa3c93234234a.fast\n 16 a193d6ef736e47faad2aa3c93234234a.fieldnorm\n256 a193d6ef736e47faad2aa3c93234234a.idx\n128 a193d6ef736e47faad2aa3c932
34234a.pos\n272 a193d6ef736e47faad2aa3c93234234a.store\n 32 a193d6ef736e47faad2aa3c93234234a.term\n  8 acf93029e3504e81beb90a47f632bba1.fast\n 16 acf93029e3504e81beb90a47f632bba1.fieldnorm\n256 acf93029e3504e81be
b90a47f632bba1.idx\n128 acf93029e3504e81beb90a47f632bba1.pos\n264 acf93029e3504e81beb90a47f632bba1.store\n 32 acf93029e3504e81beb90a47f632bba1.term\n  8 b0f37e56a12d4e328a6a903b19d84e6b.fast\n 16 b0f37e56a12d4e32
8a6a903b19d84e6b.fieldnorm\n304 b0f37e56a12d4e328a6a903b19d84e6b.idx\n144 b0f37e56a12d4e328a6a903b19d84e6b.pos\n400 b0f37e56a12d4e328a6a903b19d84e6b.store\n 32 b0f37e56a12d4e328a6a903b19d84e6b.term\n  8 b82ee7d0f
9e8439db3b73fd1889b0adc.fast\n 16 b82ee7d0f9e8439db3b73fd1889b0adc.fieldnorm\n400 b82ee7d0f9e8439db3b73fd1889b0adc.idx\n136 b82ee7d0f9e8439db3b73fd1889b0adc.pos\n392 b82ee7d0f9e8439db3b73fd1889b0adc.store\n 32 b8
2ee7d0f9e8439db3b73fd1889b0adc.term\n  0 bbee39b040fb4f1a94dcd3c2384744b2.store\n  8 eee9c251c2e24d6686fc22b262bde954.fast\n 24 eee9c251c2e24d6686fc22b262bde954.fieldnorm\n440 eee9c251c2e24d6686fc22b262bde954.idx
\n176 eee9c251c2e24d6686fc22b262bde954.pos\n392 eee9c251c2e24d6686fc22b262bde954.store\n 32 eee9c251c2e24d6686fc22b262bde954.term\n  8 meta.json\n", stderr: "" }
Tantivy index size with STORED flag = 3192118 bytes

Tantivy time required for insertion: 2.03s
```

### Infino - Apr 16, 2023 - Search benchmark with Tantivy

Infino

```
Infino time required for insertion: 2.48s
Infino index size = 1832848 bytes
Infino time required for search: 4.84ms
Number of documents with term Directory are 6837
```

Tantivy

```
Tantivy time required for insertion: 2.25s
Tantivy index size with STORED flag = 3207319 bytes
Tantivy time required for search: 17.28ms
Number of documents with term message:"Directory" are 6838
```
