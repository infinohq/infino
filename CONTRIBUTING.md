## :smile_cat: Contributing

We are so excited you are reading this section!! We welcome contributions, in all possible areas and any help is appreciated. Our [GitHub issues](https://github.com/infinohq/infino/issues) serve as a place for any discussion, whether it's bug reports, questions, project direction etc.

Just file an issue and/or raise a PR. We care about performance and tests, so please run the `benches` (in the benches folder) if you are changing anything performance sensitive, and tests as appropriate.

Feel free to discuss with the dev community on [Slack](https://infinohq.slack.com/join/shared_invite/zt-1tqqc0vsz-jF80cpkGy7aFsALQKggy8g#/shared-invite/email).


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


### Pre-commit

Before checking-in, you can run pre-commit to make sure your change passes all the tests.

* Install pre-commit by running `brew install pre-commit`
* Run `pre-commit install`

After this, tests and other checks will run before you commit.
