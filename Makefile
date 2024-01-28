prog := infino
debug ?=

ifdef debug
  $(info in debug mode, used for building non-optimized binaries...)
  release :=
else
  $(info in release mode, used for building optimized binaries...)
  release :=--release
endif

run:
	echo "Running $(prog) server..."
	cargo run $(release) --bin $(prog)

run-debug:
	echo "Running $(prog) server in debug mode..."
	RUST_LOG=debug cargo run $(release) --bin $(prog)

rust-check:
	cargo fmt --all -- --check
	cargo check
	cargo clippy --all-targets --all-features -- -D warnings

docker-check:
	@docker ps > /dev/null 2>&1 || (echo "Docker is not running. Please start Docker to run all tests." && exit 1)

test: rust-check docker-check
	echo "Running tests for all the packages"
	RUST_BACKTRACE=1 cargo test --all

build:
	cargo build $(release)

clean:
	cargo clean
	rm -rf docs/release
	rm -rf data/

.PHONY: docs

docs:
	echo "Generating documentation to docs/doc"
	cargo doc --no-deps --workspace --document-private-items --target-dir docs --release
	git add docs/doc

docker-build: docker-check
	echo "Running docker build..."
	docker build -t infinohq/infino:latest -f docker/Dockerfile .

docker-run: docker-check
	echo "Starting docker container for ${prog}..."
	docker run -it --rm -p 3000:3000 infinohq/infino:latest

docker-push: docker-check
	echo "Pushing image for ${prog}"
	docker push infinohq/infino:latest

# Rust example for indexing Apache logs.
# You can run this as below (Infino server must be running to run this example):
# `make example-apache-logs file=examples/datasets/apache-tiny.log count=100000`
example-apache-logs:
	cargo run $(release) --bin rust-apache-logs -- --file $(file) --count $(count)
