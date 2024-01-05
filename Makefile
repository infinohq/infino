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

check:
	cargo check
	cargo clippy

fmt:
	echo "Running format"
	cargo fmt --all -- --check

test: check fmt
	echo "Running test for all the packages"
	cargo test --all

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

docker-build:
	echo "Running docker build..."
	docker build -t infinohq/infino:latest -f docker/Dockerfile .

docker-run:
	echo "Starting docker container for ${prog}..."
	docker run -it --rm -p 3000:3000 infinohq/infino:latest

docker-push:
	echo "Pushing image for ${prog}"
	docker push infinohq/infino:latest
