#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys
import time
import shutil
import tarfile
import zipfile

IMAGE_NAME = "mapache-builder"
CONTAINER_NAME = "mapache-artifacts"
DOCKERFILE_PATH = "Dockerfile"
BUILD_PATH = "build"

def run_command(command, check=True):
    """Runs a shell command and streams output."""
    try:
        # We don't use capture_output=True so the user can see the progress in real-time
        result = subprocess.run(command, shell=True, check=check, text=True)
        return result
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] Command failed with exit code {e.returncode}: {command}")
        sys.exit(e.returncode)

def cleanup():
    """Removes the temporary container."""
    print("Cleaning up container...")
    subprocess.run(f"docker rm -f {CONTAINER_NAME}", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Mapache artifacts using Docker.")
    parser.add_argument("-l", "--local", action="store_true", help="Use local source from current directory.")
    parser.add_argument("-d", "--debug", action="store_true", help="Build in debug mode instead of release.")
    parser.add_argument("ref", nargs="?", default="main", help="Git ref to build (default: main).")
    parser.add_argument("features", nargs="?", default="default", help="Features to enable (default: default).")

    args = parser.parse_args()

    # Check if run as root
    if os.getuid() != 0:
        print("This script must be run with sudo.")
        sys.exit(1)

    # Get calling user and group
    sudo_user = os.environ.get("SUDO_USER")
    if sudo_user:
        calling_user = sudo_user
        # Try to get group name from user
        try:
            import grp
            import pwd
            user_info = pwd.getpwnam(calling_user)
            calling_group = grp.getgrgid(user_info.pw_gid).gr_name
        except ImportError:
            # Fallback if grp/pwd not available (unlikely on Linux)
            calling_group = calling_user
    else:
        calling_user = os.environ.get("USER") or "root"
        calling_group = calling_user

    build_source = "local" if args.local else "remote"
    ref = args.ref
    safe_ref = ref.replace("/", "-")
    features = args.features

    if build_source == "local":
        print("Using LOCAL source from current directory...")
    else:
        print(f"Using REMOTE source from Git ref: {ref} (sanitized: {safe_ref})")

    print(f"Files will belong to user: {calling_user} ({calling_group})")

    try:
        # Build the image
        cache_breaker = int(time.time())
        release_arg = f"--build-arg MAPACHE_RELEASE_BUILD=true " if not args.debug else ""
        build_cmd = (
            f"docker build "
            f"--build-arg BUILD_SOURCE={build_source} "
            f"--build-arg GIT_REF={ref} "
            f"--build-arg FEATURES={features} "
            f"{release_arg}"
            f"--build-arg CACHE_BREAKER={cache_breaker} "
            f"--tag {IMAGE_NAME} "
            f"--file {DOCKERFILE_PATH} ."
        )
        print("Building Docker image...")
        run_command(build_cmd)

        # Create temporary container
        print("Creating temporary container...")
        run_command(f"docker create --name {CONTAINER_NAME} {IMAGE_NAME}")

        # Ensure build directory exists
        if not os.path.exists(BUILD_PATH):
            os.makedirs(BUILD_PATH)

        # Change ownership of build directory
        shutil.chown(BUILD_PATH, user=calling_user, group=calling_group)

        artifacts_map = {
            f"mapache_{ref}_linux_x64": f"mapache_{safe_ref}_linux_x64",
            f"mapache_{ref}_linux_arm64": f"mapache_{safe_ref}_linux_arm64",
            f"mapache_{ref}_linux_armv7": f"mapache_{safe_ref}_linux_armv7",
            f"mapache_{ref}_win_x64.exe": f"mapache_{safe_ref}_win_x64.exe",
            f"mapache_{ref}_mac_x64": f"mapache_{safe_ref}_mac_x64",
            f"mapache_{ref}_mac_arm64": f"mapache_{safe_ref}_mac_arm64",
        }

        print("Copying and packaging artifacts...")
        container_artifacts_dir = "/artifacts"

        for src, dest in artifacts_map.items():
            dest_path = os.path.join(BUILD_PATH, dest)
            # Copy from container
            run_command(f"docker cp {CONTAINER_NAME}:{container_artifacts_dir}/{src} {dest_path}")

            # Set permissions
            if dest.endswith(".exe"):
                os.chmod(dest_path, 0o644)
            else:
                os.chmod(dest_path, 0o755)

            # Packaging
            if dest.endswith(".exe") or "mac" in dest:
                # Zip for Windows and Mac
                zip_name = dest.replace(".exe", "") + ".zip"
                zip_path = os.path.join(BUILD_PATH, zip_name)
                print(f"Creating {zip_path}...")
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    zipf.write(dest_path, os.path.basename(dest_path))
                os.remove(dest_path)
                print(f"Removed {dest_path}")
            else:
                # Tar.xz for Linux
                tar_path = dest_path + ".tar.xz"
                print(f"Creating {tar_path}...")
                with tarfile.open(tar_path, "w:xz") as tar:
                    tar.add(dest_path, arcname=os.path.basename(dest_path))
                os.remove(dest_path)
                print(f"Removed {dest_path}")

        # Final ownership and permission update
        for root, dirs, files in os.walk(BUILD_PATH):
            for d in dirs:
                shutil.chown(os.path.join(root, d), user=calling_user, group=calling_group)
            for f in files:
                shutil.chown(os.path.join(root, f), user=calling_user, group=calling_group)
                os.chmod(os.path.join(root, f), os.stat(os.path.join(root, f)).st_mode | 0o444) # Ensure readable

        print(f"Build complete! Artifacts are in the '{BUILD_PATH}' directory.")

    finally:
        cleanup()
