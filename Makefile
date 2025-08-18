REF ?= main

all: fmt debug doc test lint

debug: fmt lint doc
	@cargo build --all --all-targets

release: fmt lint doc
	@cargo build --release --all --all-targets

docker-release:
	@./build_docker.sh $(REF)

doc:
	@cargo doc --no-deps --document-private-items

test:
	@cargo test

fmt:
	@cargo fmt

lint:
	@cargo clippy

clean:
	@cargo clean
	rm -r build
