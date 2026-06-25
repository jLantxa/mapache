#!/bin/sh
set -eu

TARGET="$1"
RUSTFLAGS="${2:-}"
FEAT_ARGS="${3:-}"
TOOL="${4:-build}"

if [ -n "$RUSTFLAGS" ]; then
  VAR="CARGO_TARGET_$(echo "$TARGET" | tr '[:lower:]-' '[:upper:]_')_RUSTFLAGS"
  export "$VAR=$RUSTFLAGS"
fi

case "$TOOL" in
  build)    CMD="cargo build" ;;
  zigbuild) CMD="cargo zigbuild" ;;
  xwin)     CMD="cargo xwin build" ;;
esac

exec $CMD --release --target "$TARGET" -p mapache $FEAT_ARGS
