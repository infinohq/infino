prog := coredb
debug ?=

ifdef debug
  $(info in debug mode, used for building non-optimized binaries...)
  release :=
else
  $(info in release mode, used for building optimized binaries...)
  release :=--release
endif


check:
	cargo check
	cargo clippy

build:
	cargo build $(release)

all: build

help:
	@echo "usage: make $(prog) [debug=1]\n"
