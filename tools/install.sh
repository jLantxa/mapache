#!/bin/sh
set -eu

REPO="jLantxa/mapache"
VERSION="${VERSION:-latest}"

# ---- resolve latest version ----
if [ "$VERSION" = "latest" ]; then
  VERSION=$(curl -sSfI "https://github.com/$REPO/releases/latest" |
    grep -i location | tr -d '\r' | sed 's/.*tag\///')
fi

# ---- detect OS / arch ----
ARCH=$(uname -m)
OS=$(uname -s)

case "$OS" in
  Linux)
    INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
    BIN_NAME="mapache"
    case "$ARCH" in
      x86_64)  TARGET="linux_x64"  ; PACKAGE="tar.xz" ;;
      aarch64) TARGET="linux_arm64" ; PACKAGE="tar.xz" ;;
      armv7l)  TARGET="linux_armv7" ; PACKAGE="tar.xz" ;;
      *)       echo "Unsupported arch: $ARCH"; exit 1 ;;
    esac
    ;;
  Darwin)
    INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
    BIN_NAME="mapache"
    case "$ARCH" in
      x86_64) TARGET="mac_x64"  ; PACKAGE="zip" ;;
      arm64)  TARGET="mac_arm64" ; PACKAGE="zip" ;;
      *)      echo "Unsupported arch: $ARCH"; exit 1 ;;
    esac
    ;;
  MINGW*|MSYS*)
    INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
    BIN_NAME="mapache.exe"
    TARGET="win_x64"
    PACKAGE="zip"
    ;;
  *)
    echo "Unsupported OS: $OS"; exit 1
    ;;
esac

# ---- check dependencies ----
command -v curl >/dev/null 2>&1 || { echo "Error: curl is required"; exit 1; }
if [ "$PACKAGE" = "zip" ]; then
  command -v unzip >/dev/null 2>&1 || { echo "Error: unzip is required"; exit 1; }
fi

# ---- download ----
FILENAME="mapache_${VERSION}_${TARGET}.${PACKAGE}"
URL="https://github.com/$REPO/releases/download/$VERSION/$FILENAME"
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

echo "Downloading mapache $VERSION ($TARGET)..."
curl -sSfL "$URL" -o "$TMPDIR/$FILENAME"

# ---- extract ----
echo "Extracting..."
case "$PACKAGE" in
  tar.xz) tar -xJf "$TMPDIR/$FILENAME" -C "$TMPDIR" ;;
  zip)    unzip -o -q "$TMPDIR/$FILENAME" -d "$TMPDIR" ;;
esac

BINARY=$(find "$TMPDIR" -name "mapache*" -type f \( -name "mapache" -o -name "mapache.exe" \) 2>/dev/null | head -1)
if [ -z "$BINARY" ]; then
  # fallback: grab first matching file
  BINARY=$(find "$TMPDIR" -name "mapache*" -type f ! -name "*.zip" ! -name "*.tar.*" 2>/dev/null | head -1)
fi
if [ -z "$BINARY" ]; then
  echo "Error: mapache binary not found in archive"
  ls -la "$TMPDIR"
  exit 1
fi

# ---- install ----
chmod +x "$BINARY"
echo "Installing to $INSTALL_DIR/$BIN_NAME ..."
mkdir -p "$INSTALL_DIR" 2>/dev/null ||
  sudo mkdir -p "$INSTALL_DIR" ||
  { echo "Error: could not create directory $INSTALL_DIR"; exit 1; }

if cp "$BINARY" "$INSTALL_DIR/$BIN_NAME" 2>/dev/null; then
  :
else
  echo "Using sudo to install to $INSTALL_DIR ..."
  sudo cp "$BINARY" "$INSTALL_DIR/$BIN_NAME"
fi

echo "mapache $VERSION installed successfully!"
echo "Run 'mapache --help' to get started."
