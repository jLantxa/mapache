#!/bin/bash

# Exit script immediately on Ctrl+C
trap "echo 'Script interrupted. Exiting...'; exit 1;" SIGINT

IMAGE_NAME="mapache-builder"
CONTAINER_NAME="mapache-extract-container"
DOCKERFILE_PATH="Dockerfile"
BUILD_PATH="build"
REF=${1:-"main"}

echo "Using Git ref: $REF"

echo "Building Docker image..."
docker build --build-arg CACHE_BREAKER=$(date +%s) --build-arg GIT_REF=$REF -t $IMAGE_NAME -f $DOCKERFILE_PATH .

# Check if the last command (docker build) was successful
if [ $? -ne 0 ]; then
  echo "Docker build failed. Exiting..."
  exit 1
fi


echo "Creating container..."
docker create --name $CONTAINER_NAME $IMAGE_NAME

echo "Copying executable to host..."
mkdir -p $BUILD_PATH
chown -R $(whoami) $BUILD_PATH

LINUX_FILENAME="mapache_"$REF"_linux_x64"
WIN_FILENAME="mapache_"$REF"_win_x64"
docker cp $CONTAINER_NAME:/usr/local/bin/mapache_linux_x64 $BUILD_PATH/$LINUX_FILENAME
docker cp $CONTAINER_NAME:/usr/local/bin/mapache_win_x64.exe $BUILD_PATH/$WIN_FILENAME
zip -rj $BUILD_PATH/$LINUX_FILENAME.zip $BUILD_PATH/$LINUX_FILENAME
zip -rj $BUILD_PATH/$WIN_FILENAME.zip $BUILD_PATH/$WIN_FILENAME

echo "Cleaning up container..."
docker rm $CONTAINER_NAME

echo "Build and extraction complete."
