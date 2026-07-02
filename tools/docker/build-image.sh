#!/bin/sh
set -eu

IMAGE_NAME="${IMAGE_NAME:-mapache-builder}"
DIR="$(dirname "$0")"

exec docker build -t "$IMAGE_NAME" "$DIR"
