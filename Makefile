# Detect OS
ifeq ($(OS),Windows_NT)
    DETECTED_OS := Windows
else
    UNAME_S := $(shell uname -s)
    ifeq ($(UNAME_S),Linux)
        DETECTED_OS := Linux
    endif
    ifeq ($(UNAME_S),Darwin)
        DETECTED_OS := Darwin
    endif
endif

all: check test fmt clippy debug release

check:
	cargo check

debug:
	cargo build

release:
	cargo build --release

# The "smart" static target that works on any OS
release-static:
ifeq ($(DETECTED_OS),Windows)
	$(MAKE) release-windows
else ifeq ($(DETECTED_OS),Darwin)
	$(MAKE) release-mac
else
	$(MAKE) release-linux-static
endif

release-linux-static:
	CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_RUSTFLAGS="-C target-feature=+crt-static -C relocation-model=pie" \
		cargo build --release --target x86_64-unknown-linux-musl -p mapache

release-arm64:
	CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_RUSTFLAGS="-C target-feature=+crt-static -C relocation-model=pie" \
		cargo build --release --target aarch64-unknown-linux-musl -p mapache

release-armv7:
	CARGO_TARGET_ARMV7_UNKNOWN_LINUX_MUSLEABIHF_RUSTFLAGS="-C target-feature=+crt-static -C relocation-model=pie" \
		cargo build --release --target armv7-unknown-linux-musleabihf -p mapache

release-windows:
	RUSTFLAGS="-C target-feature=+crt-static" \
		cargo xwin build --release --target x86_64-pc-windows-msvc -p mapache

release-mac:
	cargo build --release --target x86_64-apple-darwin -p mapache --no-default-features
	cargo build --release --target aarch64-apple-darwin -p mapache --no-default-features

test:
	cargo test -r

doc:
	cargo doc --no-deps --document-private-items

fmt:
	cargo fmt --all

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

cov:
	env MAPACHE_TEST_VERBOSE=true \
		cargo llvm-cov --workspace --html --quiet \
		< /dev/null 2> /dev/null
	xdg-open ./target/llvm-cov/html/index.html

clean:
	cargo clean
	rm -rf ./build/

cloc:
	@cloc . --exclude-dir=target --timeout 0
