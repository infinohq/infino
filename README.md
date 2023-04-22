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
  <a href="https://infinohq.slack.com/join/shared_invite/zt-1tqqc0vsz-jF80cpkGy7aFsALQKggy8g#/shared-invite/email">
    <img src="https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social" alt="Join Infino Slack" />
  </a>
</p>

<p align="center">
  <a href="https://github.com/infinohq/infino/issues/new?assignees=&labels=&template=bug_report.md">Report Bug</a>
  ·
  <a href="https://github.com/infinohq/infino/issues/new?assignees=&labels=&template=feature_request.md">Request Feature</a>
</p>

<hr style="border:2px solid gray">


## :question: What is Infino?

Infino is an observability platform that stores both metrics and application logs together. The key differences between Infino and other tools are:

* It stores metrics and logs together - so you won't need to use two separate systems to store these.
* It is built for focused observability use-case and written in Rust - you will find it faster and more cost-efficient than other tools. See [benchmark](benches/README.md).


## :thinking: Why Infino?

Infino was born out of the frustation of needing two open source systems (such as Prometheus + ELK) for storing metrics and application logs, even though the purpose of these two is the same - i.e., improve obervability and reliability of systems.

When we decided to write an open-source platform that stores both of these together, we built it from the ground up in Rust, with performance (which leads to cost reduction) as **THE** key criteria to focus on. Give us a try if you'd like to store metrics and logs together, and reduce the cost of your observability infrastructure along the way!


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

### :see_no_evil: Caveat

We are currently very much an Alpha product. Please file an issue if you face any problems. Please [Contact Us](#telephone_receiver-contact-us) if you
want to discuss your use-case over virtual :coffee:.


## :punch: Contributions

Contributions are welcome in a bunch of areas and highly appreciated! To get started, check out the [contributing guidelines](CONTRIBUTING.md).

You can also join us on [Slack](https://infinohq.slack.com/join/shared_invite/zt-1tqqc0vsz-jF80cpkGy7aFsALQKggy8g#/shared-invite/email).


## :telephone_receiver: Contact Us

[Slack](https://infinohq.slack.com/join/shared_invite/zt-1tqqc0vsz-jF80cpkGy7aFsALQKggy8g#/shared-invite/email), or send an email to vinaykakade AT gmail DOT com.


## :hearts: Contributors

A big thank you to the community for making Infino possible!

<a href="https://github.com/infinohq/infino/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=infinohq/infino" />
</a>

<span style="font-size: .5rem">Made with [contrib.rocks](https://contrib.rocks).</span>
