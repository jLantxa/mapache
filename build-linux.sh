#!/bin/bash

IMAGE_NAME="mapache-builder"
CONTAINER_NAME="mapache-extract-container"
EXECUTABLE_NAME="mapache_linux_x64"
BUILD_PATH="docker/build"
REF=${1:-"main"}

echo "Using Git ref: $REF"

echo "Building Docker image..."
docker build --build-arg CACHE_BREAKER=$(date +%s) --build-arg GIT_REF=$REF -t $IMAGE_NAME -f docker/Dockerfile .

echo "Creating container..."
docker create --name $CONTAINER_NAME $IMAGE_NAME

echo "Copying executable to host..."
mkdir -p $BUILD_PATH
chown -R $(whoami) $BUILD_PATH
docker cp $CONTAINER_NAME:/usr/local/bin/mapache $BUILD_PATH/$EXECUTABLE_NAME

echo "Cleaning up container..."
docker rm $CONTAINER_NAME

echo "Build and extraction complete."
