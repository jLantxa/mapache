all: debug release test fmt clippy

debug:
	cargo build

release:
	CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_RUSTFLAGS="-C target-feature=+crt-static" \
	cargo build --release --target x86_64-unknown-linux-musl -p mapache
	cp target/x86_64-unknown-linux-musl/release/mapache target/release/mapache

test:
	cargo test -r

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
	rm -rf ./build/

