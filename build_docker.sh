#!/bin/bash
set -euo pipefail

trap "echo 'Script interrupted. Exiting...'; exit 1;" SIGINT

IMAGE_NAME="mapache-builder"
CONTAINER_NAME="mapache-extract-container"
DOCKERFILE_PATH="Dockerfile"
BUILD_PATH="build"
REF=${1:-"main"}

if [ "$(id -u)" -ne 0 ]; then
  echo "This script must be run with sudo."
  exit 1
fi

CALLING_USER="${SUDO_USER:-$(whoami)}"
CALLING_GROUP=$(id -g -n "$CALLING_USER")

echo "Using Git ref: $REF"
echo "Files will belong to user: $CALLING_USER ($CALLING_GROUP)"

cleanup() {
  echo "Cleaning up container..."
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "Building Docker image..."
docker build \
  --build-arg CACHE_BREAKER="$(date +%s)" \
  --build-arg GIT_REF="$REF" \
  -t "$IMAGE_NAME" -f "$DOCKERFILE_PATH" .

echo "Creating container..."
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
docker create --name "$CONTAINER_NAME" "$IMAGE_NAME"

mkdir -p "$BUILD_PATH"
chown -R "$CALLING_USER:$CALLING_GROUP" "$BUILD_PATH"

LINUX_FILENAME="mapache_${REF}_linux_x64"
WIN_FILENAME="mapache_${REF}_win_x64.exe"
WIN_PKGNAME="mapache_${REF}_win_x64"

docker cp "$CONTAINER_NAME:/usr/local/bin/mapache_linux_x64" "$BUILD_PATH/$LINUX_FILENAME"
docker cp "$CONTAINER_NAME:/usr/local/bin/mapache_win_x64.exe" "$BUILD_PATH/$WIN_FILENAME"

chmod 755 "$BUILD_PATH/$LINUX_FILENAME"
chmod 644 "$BUILD_PATH/$WIN_FILENAME"

tar -cJf "$BUILD_PATH/$LINUX_FILENAME.tar.xz" -C "$BUILD_PATH" "$LINUX_FILENAME"
zip -rj "$BUILD_PATH/$WIN_PKGNAME.zip" "$BUILD_PATH/$WIN_FILENAME"

chown -R "$CALLING_USER:$CALLING_GROUP" "$BUILD_PATH"
chmod -R a+r "$BUILD_PATH"

echo "Build and extraction complete."
