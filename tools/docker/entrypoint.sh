#!/bin/sh
# Entry point that runs the build as the owner of the bind-mounted source
# tree instead of as root.
#
# Building as root leaves root-owned files in /mapache (the host directory).
# If a build is interrupted before a post-build ownership fix-up runs, those
# files can only be removed with sudo. Dropping to the host user before the
# build starts means every generated file is owned by you from the outset.
set -eu

if [ "$(id -u)" = "0" ]; then
  uid="${BUILD_UID:-$(stat -c %u /mapache 2>/dev/null || echo 0)}"
  gid="${BUILD_GID:-$(stat -c %g /mapache 2>/dev/null || echo 0)}"

  if [ "$uid" != "0" ]; then
    # Repair paths that an earlier root-based build may have left root-owned:
    # the persistent cargo registry volume and the build output directory.
    # The owner check keeps this cheap on the common path.
    for d in "${CARGO_HOME:-/usr/local/cargo}/registry" /mapache/build; do
      if [ -d "$d" ] && [ "$(stat -c %u "$d" 2>/dev/null || echo "$uid")" != "$uid" ]; then
        chown -R "$uid:$gid" "$d" 2>/dev/null || true
      fi
    done

    exec setpriv --reuid="$uid" --regid="$gid" --clear-groups "$@"
  fi
fi

exec "$@"
