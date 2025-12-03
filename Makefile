all: debug test release fmt clippy

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
	env MAPACHE_TEST_QUIET=false \
		cargo llvm-cov --workspace --html --quiet \
		< /dev/null 2> /dev/null

	xdg-open ./target/llvm-cov/html/index.html

clean:
	cargo clean
