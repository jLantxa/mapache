all: debug test release fmt clippy cov

debug:
	cargo build

release:
	cargo build --release

test:
	cargo test

doc:
	cargo doc --no-deps --document-private-items

fmt:
	cargo fmt

clippy:
	cargo clippy

cov:
	env MAPACHE_TEST_QUIET=false cargo llvm-cov --html --quiet < /dev/null 2> /dev/null

clean:
	cargo clean
