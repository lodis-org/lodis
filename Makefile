format-check:
	cargo fmt --all -- --check

format:
	cargo fmt --all

build: all
	cargo build

release: all
	cargo build --release

build-publish: build publish

all: format-check
