test:
	echo "Running test for all the packages"
	cargo test --all

run:
	echo "Running optimized build"
	cargo run -r

run-debug:
	echo "Running debug (not optimized) build"
	cargo run

check:
	cargo check

fmt:
	echo "Running format"
	cargo fmt --all -- --check
