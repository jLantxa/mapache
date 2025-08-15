# mapache is an incremental backup tool
# Copyright (C) 2025  Leuqar
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

FROM fedora:latest AS builder
LABEL "Author"="Leuqar"

RUN dnf clean all && \
    dnf update -y && \
    dnf install -y \
    git \
    vim \
    cargo \
    perl \
    openssl-devel \
    fuse3-devel

# RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
# ENV PATH="/root/.cargo/bin:${PATH}"

# RUN rustup target add x86_64-pc-windows-gnu

WORKDIR /mapache

ARG CACHE_BREAKER
ARG GIT_REF="main"

RUN git clone https://github.com/jLantxa/mapache.git . && \
    cd /mapache && \
    git checkout $GIT_REF && \
    ls -l

RUN cargo test
RUN cargo build --release


FROM alpine:latest
LABEL "Author"="Leuqar"

COPY --from=builder \
    /mapache/target/release/mapache \
    /usr/local/bin/mapache_linux_x64

CMD ["/bin/true"]
