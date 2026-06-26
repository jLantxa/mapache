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
    case "$ARCH" in
      x86_64)  PLATFORM="linux"  ; ARCH_SHORT="amd64" ;;
      aarch64)
        if [ -n "${TERMUX_VERSION:-}" ]; then
          PLATFORM="android"
        else
          PLATFORM="linux"
        fi
        ARCH_SHORT="arm64"
        ;;
      armv7l)  PLATFORM="linux"  ; ARCH_SHORT="armv7" ;;
      *)       echo "Unsupported arch: $ARCH"; exit 1 ;;
    esac
    ;;
  Darwin)
    INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
    case "$ARCH" in
      x86_64) PLATFORM="darwin"  ; ARCH_SHORT="amd64" ;;
      arm64)  PLATFORM="darwin"  ; ARCH_SHORT="arm64" ;;
      *)      echo "Unsupported arch: $ARCH"; exit 1 ;;
    esac
    ;;
  MINGW*|MSYS*)
    INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"
    PLATFORM="windows"
    ARCH_SHORT="amd64"
    ;;
  *)
    echo "Unsupported OS: $OS"; exit 1
    ;;
esac

# ---- determine packaging ----
case "$PLATFORM" in
  windows) PACKAGE="zip" ; BIN_NAME="mapache.exe" ;;
  darwin)  PACKAGE="zip" ; BIN_NAME="mapache" ;;
  *)       PACKAGE="tar.xz" ; BIN_NAME="mapache" ;;
esac

# ---- check dependencies ----
command -v curl >/dev/null 2>&1 || { echo "Error: curl is required"; exit 1; }
if [ "$PACKAGE" = "zip" ]; then
  command -v unzip >/dev/null 2>&1 || { echo "Error: unzip is required"; exit 1; }
fi

# ---- download ----
FILENAME="mapache_${VERSION}_${PLATFORM}_${ARCH_SHORT}.${PACKAGE}"
URL="https://github.com/$REPO/releases/download/$VERSION/$FILENAME"
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

echo "Downloading mapache $VERSION ($PLATFORM-$ARCH_SHORT)..."
curl -sSfL "$URL" -o "$TMPDIR/$FILENAME"

# ---- extract ----
echo "Extracting..."
case "$PACKAGE" in
  tar.xz) tar -xJf "$TMPDIR/$FILENAME" -C "$TMPDIR" ;;
  zip)    unzip -o -q "$TMPDIR/$FILENAME" -d "$TMPDIR" ;;
esac

# ---- install ----
BINARY="$TMPDIR/$BIN_NAME"
if [ ! -f "$BINARY" ]; then
  echo "Error: $BIN_NAME not found in archive"
  ls -la "$TMPDIR"
  exit 1
fi

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

case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *)
    echo "Warning: $INSTALL_DIR is not in your PATH."
    echo "Add it by running:"
    echo "  echo 'export PATH=\"\$PATH:$INSTALL_DIR\"' >> \"\$HOME/.bashrc\""
    echo "  source \"\$HOME/.bashrc\""
    ;;
esac

echo "Run 'mapache --help' to get started."
