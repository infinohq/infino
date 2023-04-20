test:
	echo "Running test for all the packages"
	cargo test --all

run:
	echo "Running unoptimised build"
	cargo run

rrun:
	echo "Running optimised build"
	cargo run -r

check:
	cargo check

fmt:
	echo "Running format"
	cargo fmt --all -- --check
