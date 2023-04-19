<h1 align="center">
  Infino
</h1>

<p align="center">
  :part_alternation_mark::wood: &#151; :mag::bar_chart: &#151; :balance_scale::moneybag:
</p>

<p align="center">
Ingest Metrics and Logs &#151; Query and Insights &#151; Scale and Save $$
</p>

<hr/>

## Getting started

* Install docker
* Install pre-commit by running `brew install pre-commit`
* Run `pre-commit install`
* Run prec-commit before checking-in `pre-commit run --all-files`
* Run `cargo run`

## Developing and Contributing

We welcome contributions. Just file an issue and/or raise a PR. Feel free to discuss with the dev community on Slack -

### Code Coverage

Use [Tarpaulin](https://github.com/xd009642/tarpaulin) for code coverage.

```
$ cargo install cargo-tarpaulin
$ cargo tarpaulin
```

### Loom Test for Tsldb

```
$ RUSTFLAGS="--cfg loom" cargo test --test loom --release
```

## 💖 Contributors

A big thank you to the community for making Infino possible!

<a href="https://github.com/infinohq/infino/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=infinohq/infino" />
</a>
<span style="font-size: .5rem">Made with [contrib.rocks](https://contrib.rocks).</span>
