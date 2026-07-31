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

FROM fedora:44
LABEL "Author"="Leuqar"

RUN dnf update -y && \
  dnf install -y \
  git clang lld llvm nasm cmake unzip xz \
  musl-libc-static musl-gcc

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

RUN cargo install cargo-xwin cargo-zigbuild

# Pre-download Windows CRT/SDK (so builds don't fetch at runtime)
RUN mkdir /tmp/xwin-cache && cd /tmp/xwin-cache && \
    cargo init --name dummy && \
    RUSTFLAGS="-C target-feature=+crt-static" \
    cargo xwin build --release --target x86_64-pc-windows-msvc 2>/dev/null || true && \
    rm -rf /tmp/xwin-cache

# Download macOS SDK (repackaged from official Xcode — SHA256 verified)
RUN mkdir -p /opt/macosx-sdks && \
    curl -L https://github.com/joseluisq/macosx-sdks/releases/download/26.1/MacOSX26.1.sdk.tar.xz -o /tmp/MacOSX26.1.sdk.tar.xz && \
    echo "beee7212d265a6d2867d0236cc069314b38d5fb3486a6515734e76fa210c784c /tmp/MacOSX26.1.sdk.tar.xz" | sha256sum -c && \
    tar -xJf /tmp/MacOSX26.1.sdk.tar.xz -C /opt/macosx-sdks && \
    rm /tmp/MacOSX26.1.sdk.tar.xz

ENV SDKROOT=/opt/macosx-sdks/MacOSX26.1.sdk
ENV MACOSX_DEPLOYMENT_TARGET=10.12

# Download macFUSE library headers (for FUSE/mount support on macOS).
# The actual dylib is resolved at runtime via macFUSE on the host —
# we use -Wl,-undefined,dynamic_lookup so the linker doesn't need it.
RUN curl -L https://github.com/macfuse/library/archive/refs/heads/master.tar.gz \
      -o /tmp/library.tar.gz && \
    tar -xzf /tmp/library.tar.gz -C /opt && \
    mv /opt/library-master /opt/macfuse-library && \
    mkdir -p /opt/macosx-sdks/MacOSX26.1.sdk/usr/include/fuse && \
    cp /opt/macfuse-library/include/fuse*.h /opt/macfuse-library/include/cuse*.h /opt/macfuse-library/include/ulockmgr*.h \
      /opt/macosx-sdks/MacOSX26.1.sdk/usr/include/fuse/ && \
    mkdir -p /opt/macosx-sdks/MacOSX26.1.sdk/usr/lib/pkgconfig && \
    sed 's|@prefix@|/opt/macosx-sdks/MacOSX26.1.sdk|; \
         s|@exec_prefix@|${prefix}|; \
         s|@libdir@|${exec_prefix}/usr/lib|; \
         s|@includedir@|${prefix}/usr/include|; \
         s|@VERSION@|2.6.0|; \
         s|-lfuse|-Wl,-undefined,dynamic_lookup|; \
         s|-pthread||; \
         s|@libfuse_libs@||' \
      /opt/macfuse-library/fuse.pc.in \
      > /opt/macosx-sdks/MacOSX26.1.sdk/usr/lib/pkgconfig/fuse.pc && \
    rm -rf /tmp/library.tar.gz /opt/macfuse-library

# Download Android NDK (for Bionic C headers/linker; used directly via CC/LINKER env vars)
RUN curl -L https://dl.google.com/android/repository/android-ndk-r26d-linux.zip -o /tmp/ndk.zip && \
    unzip /tmp/ndk.zip -d /opt && \
    rm /tmp/ndk.zip
ENV ANDROID_NDK_HOME="/opt/android-ndk-r26d"
ENV CC_aarch64_linux_android="${ANDROID_NDK_HOME}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android21-clang"
ENV AR_aarch64_linux_android="${ANDROID_NDK_HOME}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ar"
ENV CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="${ANDROID_NDK_HOME}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android21-clang"

# Download Zig (used by cargo-zigbuild for cross-compilation)
ARG ZIG_VERSION=0.16.0
RUN curl -L "https://ziglang.org/download/${ZIG_VERSION}/zig-x86_64-linux-${ZIG_VERSION}.tar.xz" -o /tmp/zig.tar.xz && \
    tar -xJf /tmp/zig.tar.xz -C /opt && \
    rm /tmp/zig.tar.xz
ENV PATH="/opt/zig-x86_64-linux-${ZIG_VERSION}:${PATH}"

# Apply high optimizations
ENV CARGO_PROFILE_RELEASE_LTO="true"
ENV CARGO_PROFILE_RELEASE_CODEGEN_UNITS="1"

COPY build-target.sh /build-target.sh
COPY targets /targets

RUN for t in $(awk '!/^#|^$/{print $1}' /targets); do rustup target add "$t"; done

WORKDIR /mapache
CMD ["/bin/bash"]
