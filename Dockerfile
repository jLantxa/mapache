# mapache builder for Linux and Windows
FROM fedora:43 AS builder
LABEL "Author"="Leuqar"

RUN dnf update -y && \
    dnf install -y \
    git clang lld llvm nasm cmake \
    openssl-devel fuse3-devel

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

RUN rustup target add x86_64-pc-windows-msvc
RUN cargo install cargo-xwin

WORKDIR /mapache
ARG GIT_REF="main"
ARG FEATURES="default"

ARG CACHE_BREAKER
RUN echo "Fetching source at: $CACHE_BREAKER" && \
    git clone https://github.com/jLantxa/mapache.git /mapache && \
    cd /mapache && \
    git checkout $GIT_REF

ENV MAPACHE_RELEASE_BUILD=true

# Run tests
RUN cargo test --features $FEATURES -- --skip integration_tests::test_cmd_mount

# Build Linux Native x64
RUN cargo build --features $FEATURES --release

# Build Windows MSVC x64
ENV CC_x86_64_pc_windows_msvc=clang
ENV CXX_x86_64_pc_windows_msvc=clang++
ENV AR_x86_64_pc_windows_msvc=llvm-ar

RUN cargo xwin build --features $FEATURES --release --target x86_64-pc-windows-msvc

FROM alpine:latest
LABEL "Author"="Leuqar"

WORKDIR /artifacts

# Copy Linux binary
COPY --from=builder \
    /mapache/target/release/mapache \
    /artifacts/mapache_linux_x64

# Copy Windows binary
COPY --from=builder \
    /mapache/target/x86_64-pc-windows-msvc/release/mapache.exe \
    /artifacts/mapache_win_x64.exe

# Keep the container alive or exit cleanly
CMD ["/bin/true"]
