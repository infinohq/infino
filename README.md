<h1 align="center">
  Infino
</h1>

<p align="center">
  &nbsp;&nbsp;:part_alternation_mark::wood: &nbsp;&#151;&nbsp; :mag::bar_chart: &nbsp;&#151;&nbsp; :balance_scale::moneybag:
</p>

<p align="center">
<strong>
  Ingest Metrics and Logs &#151; Query and Insights &#151; Scale and Save\$\$
</strong>
</p>

<p align="center">
  Infino is an observability platform for storing metrics and logs at scale, and at lower cost
</p>

<hr style="border:2px solid gray">

<p align="center">
  <a href="http://www.apache.org/licenses/LICENSE-2.0.html">
    <img src="https://img.shields.io/badge/LICENSE-Apache2.0-ff69b4.svg" alt="License" />
  </a>
  <a href="https://github.com/infinohq/infino/commits">
    <img src="https://img.shields.io/github/commit-activity/m/infinohq/infino" alt="GitHub commit activity" >
  </a>
  <a href="https://infinohq.slack.com/">
    <img src="https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social" alt="Join Infino Slack" />
  </a>
</p>

<hr style="border:2px solid gray">

## :question: Why Infino?

## :fire: Features

## :beginner: Getting started

* Install docker
* Install pre-commit by running `brew install pre-commit`
* Run `pre-commit install`
* Run prec-commit before checking-in `pre-commit run --all-files`
* Run `cargo run`


## :smile_cat: Contributing

We are so excited you are reading this section!! We welcome contributions. Just file an issue and/or raise a PR.

Feel free to discuss with the dev community on [Slack](https://infinohq.slack.com/archives/C052F6DUA11).

## :punch: Developing



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
