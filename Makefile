# Set default values for variables
prog := infino
debug ?=
docker-img-tag ?= infinohq/infino:latest

.PHONY: docs docker-build docker-run docker-push docker-build-multiarch build-os-plugin clean-os-plugin clean-all

ifdef dev
	$(info in dev mode, used for building non-optimized binaries...)
	release :=
else
 	$(info in release mode, used for building optimized binaries...)
	release :=--release
endif

# Use 'make run dev' to run dev binary, or 'make run' to run release binary
run:
	echo "Running $(prog) server..."
	cargo run $(release) --bin $(prog)

run-debug-log:
	echo "Running $(prog) server in debug mode..."
	RUST_LOG=debug cargo run $(release) --bin $(prog)

rust-check:
	cargo fmt --all -- --check
	cargo check
	cargo clippy --all-targets --all-features -- -D warnings

docker-check:
	@docker ps > /dev/null 2>&1 || (echo "Docker is not running. Please start Docker to run all tests." && exit 1)

docker-buildx-check:
	@docker buildx version > /dev/null 2>&1 || (echo "Docker buildx is not available. Please install the buildx plugin to build multi-arch images." && exit 1)

test: rust-check docker-check
	echo "Running tests for all the packages"
	RUST_BACKTRACE=1 cargo test --all

build:
	cargo build $(release) 

build-os-plugin: docker-build
	cd plugins/infino-opensearch-plugin && ./gradlew build

clean-os-plugin:
	cd plugins/infino-opensearch-plugin && ./gradlew clean

clean:
	cargo clean
	rm -rf docs/release

	# index and wal directories created by 'make run'
	rm -rf data/
	rm -rf wal/

	# index and wal directories created by 'make rust-apache-logs'
	rm -fr examples/rust-apache-logs/data/
	rm -fr examples/rust-apache-logs/wal/

	# index and wal directories created by server integration tests
	rm -fr server/data/
	rm -fr server/wal/

	# clean plugin
	clean-os-plugin

docs:
	echo "Generating documentation to docs/doc"
	cargo doc --no-deps --workspace --document-private-items --target-dir docs --release
	git add docs/doc

docker-build: docker-check
	echo "Running docker build..."
	docker build --cache-from $(docker-img-tag) -t $(docker-img-tag) -f docker/infino.dockerfile .

docker-build-multiarch: docker-check docker-buildx-check
	@./scripts/build-docker-multiarch.sh --docker-img-tag $(docker-img-tag)

docker-run: docker-check
	echo "Starting docker container for ${prog}..."
	docker run -it --rm -p 3000:3000 $(docker-img-tag)

docker-push: docker-check
	echo "Pushing image for ${prog}"
	docker push $(docker-img-tag)

# Rust example for indexing Apache logs.
# You can run this as below (Infino server must be running to run this example):
# `make example-apache-logs file=../datasets/apache-tiny.log count=100000`
example-apache-logs:
	cd examples/rust-apache-logs && \
	cargo run $(release) --bin rust-apache-logs -- --file $(file) --count $(count)

# Start Infino server for memory profiling using dhat.
run-profile-server:
	echo "Building $(prog) server using profile dhat..."
	cargo build --profile dhat
	cargo run --features dhat-heap --bin $(prog)

# Run memory profiling using dhat for CoreDB.
# # You can run this as below:
# `make run-profile-coredb-only file=../datasets/apache-tiny.log count=100000`
run-profile-coredb-only:
	echo "Building $(prog) server using profile dhat..."
	cargo build --profile dhat
	cd examples/rust-apache-logs && \
        cargo run --features dhat-heap --bin rust-apache-logs -- --coredb_only --file $(file) --count $(count)
