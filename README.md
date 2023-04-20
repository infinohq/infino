<h1 align="center">
  Infino
</h1>

<p align="center">
  &nbsp;&nbsp;:part_alternation_mark::wood: &nbsp;&#151;&nbsp; :mag::bar_chart: &nbsp;&#151;&nbsp; :balance_scale::moneybag:
</p>

<p align="center">
<strong>
  Ingest Metrics and Logs &#151; Query and Insights &#151; Scale and Save &dollar;&dollar;
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

## :question: What is Infino?

Infino is an observability platform that stores both metrics and application logs together. The key differences in Infino and other tools are:

* it stores metrics and logs together - so you won't need to use two separate systems to store these,
* it is built for focused observability use-case and written in Rust - you will find it faster and more cost efficient than other tools. See [benchmark](benches/README.md).

## :thinking: Why Infino?

Infino was born out of the frustation of needing two open source systems (such as Prometheus + ELK) for storing metrics and application logs, even though
the purpose of these two is the same - i.e., improve obervability and reliability of systems.

When we decided to write an open-source platform that stores both of these together, we built it from the ground up in Rust, with performance (which leads
to cost reduction) as **THE** key criteria to focus on. Give us a try if you'd like to store metrics and logs together, and reduce the cost of your
observability infrastructure along the way!

## :fire: Features

* Store metrics and logs together,
* Ingest using [FluentBit](https://fluentbit.io/),
* Search logs using boolean *AND* queries,
* Query time series of metrics stored,
* Coming Soon:
  * Store metrics and logs on S3,
  * More integrations: Prometheus, LogStash, and Grafana,
  * Powerful query language for metrics and logs,
  * Performance improvements while searching logs and metrics.
  * Inbuilt queue for disaster recovery - no data loss in case of failures.
  * Clients in other languages - Java, JavaScipt, Python,
  * UI for querying Infino.
  * Support for traces and Spans

## :beginner: Getting started

* Install [Docker](https://docs.docker.com/engine/install/).
* Install [Rust toolchain](https://www.rust-lang.org/tools/install).
* TODO: add details regarding make targets.

## :see_no_evil: Caveat

We are currently very much an Alpha product. Please file an issue if you face any problems. Please [Contact Us](#telephone_receiver-contact-us) if you
want to discuss your use-case over virtual :coffee:.

## :punch: Developing

### Setup

* Install [Docker](https://docs.docker.com/engine/install/).
* [Install](https://www.rust-lang.org/tools/install) the Rust toolchain.
* Clone this repo.
* Setup your preferred IDE with formatting rules in `rustfmt.toml`. We use default rustfmt formatting, except for using 2 spaces for alignment instead of 4.
If you are using VSCode, the `Rust Analyzer` plugin with pickup the `rustfmt.toml` from the repo and format any code changes you make.
* Start Infino service using `make run` command

### Run tests

The core database behind Infino is Time Series and Log DB (Tsldb), which is cargo workspace crate in this repo in `tsldb` folder. We run
tests for both Infino server and Tsldb below:

```
$ make test
```

### Code Coverage

You can find code coverage using [Tarpaulin](https://github.com/xd009642/tarpaulin).

```
$ cargo install cargo-tarpaulin
$ cargo tarpaulin
```

### Loom test for Tsldb

Tsldb is a multithreaded application. In addition to exchaustive functional tests, we use [Loom](https://docs.rs/loom/latest/loom/) to test permutations of threads and data access.

```
$ RUSTFLAGS="--cfg loom" cargo test --test loom --release
```

## :smile_cat: Contributing

We are so excited you are reading this section!! We welcome contributions. Just file an issue and/or raise a PR. We care about performance and tests,
so please run the `benches` (in the benches folder) if you are changing anything performance sensitive, and tests as appropriate.

Feel free to discuss with the dev community on [Slack](https://infinohq.slack.com/archives/C052F6DUA11).

### Pre-commit

Before checking-in, you can run pre-commit to make sure your change passes all the tests.

* Install pre-commit by running `brew install pre-commit`
* Run `pre-commit install`

After this, tests and other checks will run before you commit.

## :telephone_receiver: Contact Us

[Slack](https://infinohq.slack.com/archives/C052F6DUA11), or send an email to vinaykakade AT gmail DOT com.

## :hearts: Contributors

A big thank you to the community for making Infino possible!

<a href="https://github.com/infinohq/infino/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=infinohq/infino" />
</a>

<span style="font-size: .5rem">Made with [contrib.rocks](https://contrib.rocks).</span>
