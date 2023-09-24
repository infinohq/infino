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
	cargo run $(release)

check:
	cargo check

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

docker-build:
	echo "Running docker build..."
	docker build -t infinohq/infino:latest -f docker/Dockerfile .

docker-run:
	echo "Starting docker container for ${prog}..."
	docker run -it --rm -p 3000:3000 infinohq/infino:latest

docker-push:
	echo "Pushing image for ${prog}"
	docker push infinohq/infino:latest
