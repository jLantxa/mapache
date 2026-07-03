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

FEATURES ?= default
BUILD_SH := sh tools/docker/build-target.sh

all: check test fmt clippy debug release

check:
	cargo check

debug:
	cargo build

release: release-static

release-static:
ifeq ($(DETECTED_OS),Windows)
	$(MAKE) release-windows
else ifeq ($(DETECTED_OS),Darwin)
	$(MAKE) release-darwin
else
	$(MAKE) release-linux-static
endif

release-linux-static:
	$(BUILD_SH) x86_64-unknown-linux-musl \
		"-C target-feature=+crt-static -C relocation-model=pie" \
		"--features $(FEATURES)" build

release-arm64:
	$(BUILD_SH) aarch64-unknown-linux-musl \
		"-C target-feature=+crt-static -C relocation-model=pie" \
		"--features $(FEATURES)" zigbuild

release-android-arm64:
ifndef ANDROID_NDK_HOME
	$(error ANDROID_NDK_HOME is not set. Install Android NDK and point ANDROID_NDK_HOME to it)
endif
	$(BUILD_SH) aarch64-linux-android "" \
		"--no-default-features" build

release-armv7:
	$(BUILD_SH) armv7-unknown-linux-musleabihf \
		"-C target-feature=+crt-static -C relocation-model=pie" \
		"--features $(FEATURES)" zigbuild

release-windows:
	RUSTFLAGS="-C target-feature=+crt-static" \
		$(BUILD_SH) x86_64-pc-windows-msvc "" \
		"--features $(FEATURES)" xwin

# NOTE: macOS binaries are NOT fully self-contained — Apple's system libraries
# cannot be statically linked. These will depend on system dylibs.
release-darwin:
	$(BUILD_SH) x86_64-apple-darwin "" "--no-default-features" zigbuild
	$(BUILD_SH) aarch64-apple-darwin "" "--no-default-features" zigbuild

audit:
	cargo audit --deny warnings

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
