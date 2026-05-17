# mapache builder for Linux (x64, ARM64, ARMv7), Windows, and Mac
# We define ARGs at the top so they can be used in FROM
ARG BUILD_SOURCE="remote"

FROM fedora:43 AS build-env
LABEL "Author"="Leuqar"

RUN dnf update -y && \
  dnf install -y \
  git clang lld llvm nasm cmake \
  musl-libc-static musl-gcc zig

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

RUN rustup target add x86_64-unknown-linux-musl \
    aarch64-unknown-linux-musl \
    armv7-unknown-linux-musleabihf \
    x86_64-pc-windows-msvc \
    x86_64-apple-darwin \
    aarch64-apple-darwin

RUN cargo install cargo-xwin cargo-zigbuild

# Download macOS SDK
RUN mkdir -p /opt/macosx-sdks && \
    curl -L https://github.com/phracker/MacOSX-SDKs/releases/download/11.3/MacOSX11.3.sdk.tar.xz | tar -xJ -C /opt/macosx-sdks

ENV SDKROOT=/opt/macosx-sdks/MacOSX11.3.sdk

# --- Stage: Remote Source ---
FROM build-env AS source-remote
ARG GIT_REF="main"
ARG CACHE_BREAKER
RUN echo "Fetching remote source from $GIT_REF (cache breaker: $CACHE_BREAKER)..." && \
    git clone https://github.com/jLantxa/mapache.git /mapache && \
    cd /mapache && git checkout $GIT_REF

# --- Stage: Local Source ---
FROM build-env AS source-local
COPY . /mapache

# --- Stage: Final Builder (Selective) ---
FROM source-${BUILD_SOURCE} AS builder
WORKDIR /mapache
ARG FEATURES="default"
ARG MAPACHE_RELEASE_BUILD

# Run tests
RUN if [ -n "$MAPACHE_RELEASE_BUILD" ]; then \
    MAPACHE_RELEASE_BUILD="$MAPACHE_RELEASE_BUILD" cargo test --features $FEATURES --release -- --skip integration_tests::test_cmd_mount; \
    else \
    cargo test --features $FEATURES --release -- --skip integration_tests::test_cmd_mount; \
    fi

# Apply high optimizations
ENV CARGO_PROFILE_RELEASE_LTO="true"
ENV CARGO_PROFILE_RELEASE_CODEGEN_UNITS="1"

# Build Linux x64 (Static Musl)
RUN if [ -n "$MAPACHE_RELEASE_BUILD" ]; then \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_RUSTFLAGS="-C target-feature=+crt-static" \
    MAPACHE_RELEASE_BUILD="$MAPACHE_RELEASE_BUILD" cargo build --features $FEATURES --release --target x86_64-unknown-linux-musl -p mapache; \
    else \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_RUSTFLAGS="-C target-feature=+crt-static" \
    cargo build --features $FEATURES --release --target x86_64-unknown-linux-musl -p mapache; \
    fi

# Build Linux ARM64 (Static Musl)
RUN if [ -n "$MAPACHE_RELEASE_BUILD" ]; then \
    MAPACHE_RELEASE_BUILD="$MAPACHE_RELEASE_BUILD" cargo zigbuild --features $FEATURES --release --target aarch64-unknown-linux-musl -p mapache; \
    else \
    cargo zigbuild --features $FEATURES --release --target aarch64-unknown-linux-musl -p mapache; \
    fi

# Build Linux ARMv7 (Static Musl)
RUN if [ -n "$MAPACHE_RELEASE_BUILD" ]; then \
    MAPACHE_RELEASE_BUILD="$MAPACHE_RELEASE_BUILD" cargo zigbuild --features $FEATURES --release --target armv7-unknown-linux-musleabihf -p mapache; \
    else \
    cargo zigbuild --features $FEATURES --release --target armv7-unknown-linux-musleabihf -p mapache; \
    fi

# Build Windows MSVC x64 (Static CRT)
RUN if [ -n "$MAPACHE_RELEASE_BUILD" ]; then \
    RUSTFLAGS="-C target-feature=+crt-static" \
    MAPACHE_RELEASE_BUILD="$MAPACHE_RELEASE_BUILD" cargo xwin build --features $FEATURES --release --target x86_64-pc-windows-msvc -p mapache; \
    else \
    RUSTFLAGS="-C target-feature=+crt-static" \
    cargo xwin build --features $FEATURES --release --target x86_64-pc-windows-msvc -p mapache; \
    fi

# Build Mac Intel and Apple Silicon
RUN if [ -n "$MAPACHE_RELEASE_BUILD" ]; then \
    MAPACHE_RELEASE_BUILD="$MAPACHE_RELEASE_BUILD" cargo zigbuild --release --target x86_64-apple-darwin -p mapache --no-default-features; \
    else \
    cargo zigbuild --release --target x86_64-apple-darwin -p mapache --no-default-features; \
    fi
RUN if [ -n "$MAPACHE_RELEASE_BUILD" ]; then \
    MAPACHE_RELEASE_BUILD="$MAPACHE_RELEASE_BUILD" cargo zigbuild --release --target aarch64-apple-darwin -p mapache --no-default-features; \
    else \
    cargo zigbuild --release --target aarch64-apple-darwin -p mapache --no-default-features; \
    fi

# --- Final Image ---
FROM alpine:latest
LABEL "Author"="Leuqar"

ARG GIT_REF="unknown"

# Add non-root user
RUN addgroup -S mapache && adduser -S mapache -G mapache
USER mapache

WORKDIR /artifacts

COPY --from=builder /mapache/target/x86_64-unknown-linux-musl/release/mapache /artifacts/mapache_${GIT_REF}_linux_x64
COPY --from=builder /mapache/target/aarch64-unknown-linux-musl/release/mapache /artifacts/mapache_${GIT_REF}_linux_arm64
COPY --from=builder /mapache/target/armv7-unknown-linux-musleabihf/release/mapache /artifacts/mapache_${GIT_REF}_linux_armv7
COPY --from=builder /mapache/target/x86_64-pc-windows-msvc/release/mapache.exe /artifacts/mapache_${GIT_REF}_win_x64.exe
COPY --from=builder /mapache/target/x86_64-apple-darwin/release/mapache /artifacts/mapache_${GIT_REF}_mac_x64
COPY --from=builder /mapache/target/aarch64-apple-darwin/release/mapache /artifacts/mapache_${GIT_REF}_mac_arm64

CMD ["/bin/true"]
