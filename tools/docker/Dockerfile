# mapache cross-compilation environment
# Use this image to compile mapache for any supported target.
#
# Usage:
#   docker build -t mapache-builder tools/docker
#
#   # Build a single target (source mounted from host)
#   docker run --rm -v $(pwd):/mapache mapache-builder \
#     sh /build-target.sh x86_64-unknown-linux-musl \
#       "-C target-feature=+crt-static" \
#       build
#
#   # Interactive development shell
#   docker run --rm -it -v $(pwd):/mapache mapache-builder
#
# The container entry point (tools/docker/entrypoint.sh) drops privileges to
# the owner of the mounted /mapache directory, so build artifacts are never
# left owned by root — even if a build is interrupted. Override with
# `-e BUILD_UID=$(id -u) -e BUILD_GID=$(id -g)` if needed.
#
# Toolchain versions are pinned below so the image is reproducible. When
# bumping any versioned argument, update the matching checksum too.

ARG FEDORA_VERSION=44
FROM fedora:${FEDORA_VERSION}
LABEL "Author"="Leuqar"

# Redeclare build args after FROM so they are in scope for the build stage.
ARG CARGO_ZIGBUILD_VERSION=0.23.0
ARG CARGO_XWIN_VERSION=0.23.1
ARG ZIG_VERSION=0.16.0
# https://ziglang.org/download/index.json (x86_64-linux)
ARG ZIG_SHA256=70e49664a74374b48b51e6f3fdfbf437f6395d42509050588bd49abe52ba3d00
ARG NDK_VERSION=r26d
# Google only publishes a SHA1 for NDK artifacts.
ARG NDK_SHA1=fcdad75a765a46a9cf6560353f480db251d14765
ARG MACOSX_SDK_VERSION=26.1
ARG MACOSX_SDK_SHA256=beee7212d265a6d2867d0236cc069314b38d5fb3486a6515734e76fa210c784c

RUN dnf update -y && \
  dnf install -y \
  git clang lld llvm nasm cmake unzip xz \
  musl-libc-static musl-gcc && \
  dnf clean all

# Install the Rust toolchain under /usr/local (not /root) so an arbitrary
# build UID can read and execute it. HOME points at CARGO_HOME because it is
# writable by the build user.
ENV RUSTUP_HOME=/usr/local/rustup \
  CARGO_HOME=/usr/local/cargo \
  HOME=/usr/local/cargo \
  PATH="/usr/local/cargo/bin:${PATH}"

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path

RUN cargo install cargo-xwin --version "=${CARGO_XWIN_VERSION}" && \
  cargo install cargo-zigbuild --version "=${CARGO_ZIGBUILD_VERSION}"

# Pre-download Windows CRT/SDK into a cache the build user can write to
# (so builds don't fetch at runtime).
ENV XWIN_CACHE_DIR=/opt/cargo-xwin-cache
RUN mkdir -p "$XWIN_CACHE_DIR" /tmp/xwin-warm && cd /tmp/xwin-warm && \
    cargo init --name dummy && \
    RUSTFLAGS="-C target-feature=+crt-static" \
    cargo xwin build --release --target x86_64-pc-windows-msvc 2>/dev/null || true && \
    cd / && rm -rf /tmp/xwin-warm

# Download macOS SDK (repackaged from official Xcode — SHA256 verified)
RUN mkdir -p /opt/macosx-sdks && \
    curl -L "https://github.com/joseluisq/macosx-sdks/releases/download/${MACOSX_SDK_VERSION}/MacOSX${MACOSX_SDK_VERSION}.sdk.tar.xz" -o /tmp/macosx-sdk.tar.xz && \
    echo "${MACOSX_SDK_SHA256}  /tmp/macosx-sdk.tar.xz" | sha256sum -c && \
    tar -xJf /tmp/macosx-sdk.tar.xz -C /opt/macosx-sdks && \
    rm /tmp/macosx-sdk.tar.xz

ENV SDKROOT=/opt/macosx-sdks/MacOSX${MACOSX_SDK_VERSION}.sdk
ENV MACOSX_DEPLOYMENT_TARGET=10.12

# Download macFUSE library headers (for FUSE/mount support on macOS).
# The actual dylib is resolved at runtime via macFUSE on the host —
# we use -Wl,-undefined,dynamic_lookup so the linker doesn't need it.
RUN curl -L https://github.com/macfuse/library/archive/refs/heads/master.tar.gz \
      -o /tmp/library.tar.gz && \
    tar -xzf /tmp/library.tar.gz -C /opt && \
    mv /opt/library-master /opt/macfuse-library && \
    mkdir -p /opt/macosx-sdks/MacOSX${MACOSX_SDK_VERSION}.sdk/usr/include/fuse && \
    cp /opt/macfuse-library/include/fuse*.h /opt/macfuse-library/include/cuse*.h /opt/macfuse-library/include/ulockmgr*.h \
      /opt/macosx-sdks/MacOSX${MACOSX_SDK_VERSION}.sdk/usr/include/fuse/ && \
    mkdir -p /opt/macosx-sdks/MacOSX${MACOSX_SDK_VERSION}.sdk/usr/lib/pkgconfig && \
    sed "s|@prefix@|/opt/macosx-sdks/MacOSX${MACOSX_SDK_VERSION}.sdk|; \
         s|@exec_prefix@|\${prefix}|; \
         s|@libdir@|\${exec_prefix}/usr/lib|; \
         s|@includedir@|\${prefix}/usr/include|; \
         s|@VERSION@|2.6.0|; \
         s|-lfuse|-Wl,-undefined,dynamic_lookup|; \
         s|-pthread||; \
         s|@libfuse_libs@||" \
       /opt/macfuse-library/fuse.pc.in \
       > /opt/macosx-sdks/MacOSX${MACOSX_SDK_VERSION}.sdk/usr/lib/pkgconfig/fuse.pc && \
    rm -rf /tmp/library.tar.gz /opt/macfuse-library

# Download Android NDK (for Bionic C headers/linker; used directly via CC/LINKER env vars)
RUN curl -L "https://dl.google.com/android/repository/android-ndk-${NDK_VERSION}-linux.zip" -o /tmp/ndk.zip && \
    echo "${NDK_SHA1}  /tmp/ndk.zip" | sha1sum -c && \
    unzip -q /tmp/ndk.zip -d /opt && \
    rm /tmp/ndk.zip
ENV ANDROID_NDK_HOME="/opt/android-ndk-${NDK_VERSION}"
ENV CC_aarch64_linux_android="${ANDROID_NDK_HOME}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android21-clang"
ENV AR_aarch64_linux_android="${ANDROID_NDK_HOME}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ar"
ENV CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="${ANDROID_NDK_HOME}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android21-clang"

# Download Zig (used by cargo-zigbuild for cross-compilation)
RUN curl -L "https://ziglang.org/download/${ZIG_VERSION}/zig-x86_64-linux-${ZIG_VERSION}.tar.xz" -o /tmp/zig.tar.xz && \
    echo "${ZIG_SHA256}  /tmp/zig.tar.xz" | sha256sum -c && \
    tar -xJf /tmp/zig.tar.xz -C /opt && \
    rm /tmp/zig.tar.xz
ENV PATH="/opt/zig-x86_64-linux-${ZIG_VERSION}:${PATH}"

# Keep Zig's global cache off $HOME/.cache so it is stable regardless of UID.
ENV ZIG_GLOBAL_CACHE_DIR=/opt/zig-cache
RUN mkdir -p "$ZIG_GLOBAL_CACHE_DIR"

# Apply high optimizations
ENV CARGO_PROFILE_RELEASE_LTO="true"
ENV CARGO_PROFILE_RELEASE_CODEGEN_UNITS="1"

COPY build-target.sh /build-target.sh
COPY entrypoint.sh /entrypoint.sh
COPY targets /targets

RUN for t in $(awk '!/^#|^$/{print $1}' /targets); do rustup target add "$t"; done

# Make the toolchain readable/executable by any UID and the caches writable,
# so the container can run as the (arbitrary) host user instead of root.
RUN chmod -R a+rX /usr/local/rustup /usr/local/cargo && \
  chmod -R a+rwX /usr/local/cargo "$XWIN_CACHE_DIR" "$ZIG_GLOBAL_CACHE_DIR" && \
  chmod +x /entrypoint.sh

WORKDIR /mapache
ENTRYPOINT ["/entrypoint.sh"]
CMD ["/bin/bash"]
