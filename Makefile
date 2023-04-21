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
