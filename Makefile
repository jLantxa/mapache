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

	grcov target/llvm-cov-target/ \
		-s . \
		--binary-path ./target/llvm-cov-target/debug/deps/ \
		-t html \
		-o ./target/grcov/

	xdg-open ./target/llvm-cov/html/index.html
	xdg-open ./target/grcov/index.html

clean:
	cargo clean
