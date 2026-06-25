# mapache builder for Linux (x64, ARM64, ARMv7), Windows, and Mac
# We define ARGs at the top so they can be used in FROM
ARG BUILD_SOURCE="remote"

FROM fedora:44 AS build-env
LABEL "Author"="Leuqar"

RUN dnf update -y && \
  dnf install -y \
  git clang lld llvm nasm cmake unzip xz \
  musl-libc-static musl-gcc

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

RUN rustup target add x86_64-unknown-linux-musl \
    aarch64-unknown-linux-musl \
    armv7-unknown-linux-musleabihf \
    aarch64-linux-android \
    x86_64-pc-windows-msvc \
    x86_64-apple-darwin \
    aarch64-apple-darwin

RUN cargo install cargo-xwin cargo-zigbuild

# Download macOS SDK
RUN mkdir -p /opt/macosx-sdks && \
    curl -L https://github.com/phracker/MacOSX-SDKs/releases/download/11.3/MacOSX11.3.sdk.tar.xz | tar -xJ -C /opt/macosx-sdks

ENV SDKROOT=/opt/macosx-sdks/MacOSX11.3.sdk

# Download Android NDK (for Bionic C headers/linker; used directly via CC/LINKER env vars)
RUN curl -L https://dl.google.com/android/repository/android-ndk-r26d-linux.zip -o /tmp/ndk.zip && \
    unzip /tmp/ndk.zip -d /opt && \
    rm /tmp/ndk.zip
ENV ANDROID_NDK_HOME="/opt/android-ndk-r26d"
ENV CC_aarch64_linux_android="${ANDROID_NDK_HOME}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android21-clang"
ENV AR_aarch64_linux_android="${ANDROID_NDK_HOME}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ar"
ENV CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="${ANDROID_NDK_HOME}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android21-clang"

# Download Zig (used by cargo-zigbuild for cross-compilation)
RUN curl -L https://ziglang.org/download/0.14.0/zig-linux-x86_64-0.14.0.tar.xz -o /tmp/zig.tar.xz && \
    tar -xJf /tmp/zig.tar.xz -C /opt && \
    rm /tmp/zig.tar.xz
ENV PATH="/opt/zig-linux-x86_64-0.14.0:${PATH}"

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

COPY tools/docker/build-target.sh /build-target.sh

# Run tests
RUN cargo test --features "$FEATURES" --release -- --skip integration_tests::test_cmd_mount

# Apply high optimizations
ENV CARGO_PROFILE_RELEASE_LTO="true"
ENV CARGO_PROFILE_RELEASE_CODEGEN_UNITS="1"

# Build Linux x64 (Static Musl)
RUN sh /build-target.sh x86_64-unknown-linux-musl \
    "-C target-feature=+crt-static -C relocation-model=pie -C link-arg=-pie" \
    "${FEATURES:+--features $FEATURES}" build

# Build Linux ARM64 (Static Musl)
RUN sh /build-target.sh aarch64-unknown-linux-musl \
    "-C target-feature=+crt-static -C relocation-model=pie" \
    "${FEATURES:+--features $FEATURES}" zigbuild

# Build ARM64 (Android Bionic)
RUN sh /build-target.sh aarch64-linux-android "" \
    "--no-default-features --features tui" build

# Build Linux ARMv7 (Static Musl)
RUN sh /build-target.sh armv7-unknown-linux-musleabihf \
    "-C relocation-model=pie -C link-arg=-pie" \
    "${FEATURES:+--features $FEATURES}" zigbuild

# Build Windows x64 (Static CRT)
RUN RUSTFLAGS="-C target-feature=+crt-static" \
    sh /build-target.sh x86_64-pc-windows-msvc "" \
    "${FEATURES:+--features $FEATURES}" xwin

# Build Mac x86_64 and ARM64
RUN sh /build-target.sh x86_64-apple-darwin "" \
    "--no-default-features" zigbuild
RUN sh /build-target.sh aarch64-apple-darwin "" \
    "--no-default-features" zigbuild

# --- Final Image ---
FROM alpine:latest
LABEL "Author"="Leuqar"

ARG GIT_REF="unknown"

# Add non-root user
RUN addgroup -S mapache && adduser -S mapache -G mapache
USER mapache

WORKDIR /artifacts

COPY --from=builder /mapache/target/x86_64-unknown-linux-musl/release/mapache /artifacts/mapache-linux-amd64
COPY --from=builder /mapache/target/aarch64-unknown-linux-musl/release/mapache /artifacts/mapache-linux-arm64
COPY --from=builder /mapache/target/aarch64-linux-android/release/mapache /artifacts/mapache-android-arm64
COPY --from=builder /mapache/target/armv7-unknown-linux-musleabihf/release/mapache /artifacts/mapache-linux-armv7
COPY --from=builder /mapache/target/x86_64-pc-windows-msvc/release/mapache.exe /artifacts/mapache-windows-amd64.exe
COPY --from=builder /mapache/target/x86_64-apple-darwin/release/mapache /artifacts/mapache-darwin-amd64
COPY --from=builder /mapache/target/aarch64-apple-darwin/release/mapache /artifacts/mapache-darwin-arm64

CMD ["/bin/true"]
