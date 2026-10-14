#!/bin/sh
set -eu

IMAGE_NAME="${IMAGE_NAME:-mapache-builder}"
TAG="${TAG:-latest}"
DIR="$(dirname "$0")"

exec docker build --pull --no-cache -t "${IMAGE_NAME}:${TAG}" -t "${IMAGE_NAME}:latest" "$DIR"
