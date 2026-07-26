//! `FileTime` and helpers to write atime/mtime on files.
//!
//! ## Platform notes
//! - **Linux** – `libc::utimensat` with `AT_FDCWD`.
//! - **macOS / other Unix** – same `utimensat` path (POSIX.1-2008).
//! - **Windows** – `CreateFileW` + `SetFileTime`.

use std::{io, path::Path, time::SystemTime};

/// A file timestamp represented as seconds + nanoseconds since the Unix epoch.
#[derive(Copy, Clone, Debug)]
pub struct FileTime {
    seconds: i64,
    nanos: u32,
}

impl FileTime {
    /// Epoch (1970-01-01 00:00:00 UTC).
    pub fn zero() -> Self {
        FileTime {
            seconds: 0,
            nanos: 0,
        }
    }

    /// Build from a raw Unix timestamp.
    pub fn from_unix_time(secs: i64, nsecs: u32) -> Self {
        FileTime {
            seconds: secs,
            nanos: nsecs,
        }
    }

    /// Whole seconds since the epoch (may be negative for pre-1970 dates).
    pub fn unix_seconds(&self) -> i64 {
        self.seconds
    }

    /// Nanosecond fraction (always 0..1_000_000_000).
    pub fn unix_nanos(&self) -> u32 {
        self.nanos
    }

    /// Extract the modification time from `fs::Metadata`.
    ///
    /// On Unix this reads the raw `mtime`/`mtime_nsec` fields for sub-second
    /// precision; on Windows it falls back to `Metadata::modified()`.
    pub fn from_last_modification_time(meta: &std::fs::Metadata) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            FileTime {
                seconds: meta.mtime(),
                nanos: meta.mtime_nsec() as u32,
            }
        }
        #[cfg(not(unix))]
        {
            meta.modified().unwrap_or(SystemTime::UNIX_EPOCH).into()
        }
    }
}

impl From<SystemTime> for FileTime {
    fn from(time: SystemTime) -> Self {
        let duration = time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        FileTime {
            seconds: duration.as_secs() as i64,
            nanos: duration.subsec_nanos(),
        }
    }
}

impl From<&SystemTime> for FileTime {
    fn from(time: &SystemTime) -> Self {
        (*time).into()
    }
}

#[cfg(unix)]
mod unix {
    use std::{ffi::CString, io, os::unix::ffi::OsStrExt, path::Path};

    use super::FileTime;

    pub fn set_file_times_impl(
        path: &Path,
        atime: FileTime,
        mtime: FileTime,
        follow_symlinks: bool,
    ) -> io::Result<()> {
        let flags = if follow_symlinks {
            0
        } else {
            libc::AT_SYMLINK_NOFOLLOW
        };

        let cpath = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains null byte"))?;

        let times = [
            libc::timespec {
                tv_sec: atime.seconds as _,
                tv_nsec: atime.nanos as libc::c_long,
            },
            libc::timespec {
                tv_sec: mtime.seconds as _,
                tv_nsec: mtime.nanos as libc::c_long,
            },
        ];

        let ret = unsafe {
            // SAFETY: FFI call to utimensat with valid null-terminated path and timespec array.
            libc::utimensat(libc::AT_FDCWD, cpath.as_ptr(), times.as_ptr(), flags)
        };

        if ret != 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

#[cfg(windows)]
mod windows {
    use std::{io, os::windows::ffi::OsStrExt, path::Path};

    use super::FileTime;

    // FILETIME epoch is 1601-01-01; Unix epoch is 1970-01-01.
    const UNIX_TO_FILETIME_SECS: i64 = 11644473600;

    /// Number of 100-ns ticks in one second.
    const TICKS_PER_SEC: u64 = 10_000_000;

    fn unix_to_filetime(ft: FileTime) -> windows_sys::Win32::Foundation::FILETIME {
        let secs = ft
            .seconds
            .checked_add(UNIX_TO_FILETIME_SECS)
            .expect("FILETIME overflow: timestamp too large");
        let total = secs
            .checked_mul(TICKS_PER_SEC as i64)
            .and_then(|v| v.checked_add((ft.nanos / 100) as i64))
            .expect("FILETIME overflow: timestamp too large");
        let total = total as u64;
        windows_sys::Win32::Foundation::FILETIME {
            dwLowDateTime: total as u32,
            dwHighDateTime: (total >> 32) as u32,
        }
    }

    pub fn set_file_times_impl(path: &Path, atime: FileTime, mtime: FileTime) -> io::Result<()> {
        let wide: Vec<u16> = path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();

        // SAFETY: wide path is null-terminated; handle is checked before use.
        let handle = unsafe {
            windows_sys::Win32::Storage::FileSystem::CreateFileW(
                wide.as_ptr(),
                windows_sys::Win32::Storage::FileSystem::FILE_WRITE_ATTRIBUTES,
                windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ
                    | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_WRITE,
                std::ptr::null(),
                windows_sys::Win32::Storage::FileSystem::OPEN_EXISTING,
                windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS,
                std::ptr::null_mut(),
            )
        };

        if handle == windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }

        let atime_ft = unix_to_filetime(atime);
        let mtime_ft = unix_to_filetime(mtime);

        // SAFETY: handle is valid; FILETIME values are properly initialised.
        let ret = unsafe {
            windows_sys::Win32::Storage::FileSystem::SetFileTime(
                handle,
                std::ptr::null(),
                &atime_ft,
                &mtime_ft,
            )
        };

        // SAFETY: CloseHandle is safe to call regardless of SetFileTime return.
        unsafe { windows_sys::Win32::Foundation::CloseHandle(handle) };

        if ret == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

/// Set the access and modification times of a regular file (or directory).
///
/// On Unix this follows symlinks; on Windows it follows reparse points.
pub fn set_file_times<P: AsRef<Path>>(path: P, atime: FileTime, mtime: FileTime) -> io::Result<()> {
    let path = path.as_ref();
    #[cfg(unix)]
    {
        unix::set_file_times_impl(path, atime, mtime, true)
    }
    #[cfg(windows)]
    {
        windows::set_file_times_impl(path, atime, mtime)
    }
}

/// Set the access and modification times **without** following symlinks.
///
/// On Windows this is identical to `set_file_times` because the Windows API
/// does not provide a symlink-specific timestamp write.
#[cfg(unix)]
pub fn set_symlink_file_times<P: AsRef<Path>>(
    path: P,
    atime: FileTime,
    mtime: FileTime,
) -> io::Result<()> {
    unix::set_file_times_impl(path.as_ref(), atime, mtime, false)
}

/// Set the access and modification times **without** following symlinks.
///
/// On Windows this is identical to `set_file_times` because the Windows API
/// does not provide a symlink-specific timestamp write.
#[cfg(windows)]
pub fn set_symlink_file_times<P: AsRef<Path>>(
    path: P,
    atime: FileTime,
    mtime: FileTime,
) -> io::Result<()> {
    windows::set_file_times_impl(path.as_ref(), atime, mtime)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn zero_epoch() {
        let ft = FileTime::zero();
        assert_eq!(ft.unix_seconds(), 0);
        assert_eq!(ft.unix_nanos(), 0);
    }

    #[test]
    fn from_unix_time_roundtrip() {
        let ft = FileTime::from_unix_time(1_234_567_890, 123_456_789);
        assert_eq!(ft.unix_seconds(), 1_234_567_890);
        assert_eq!(ft.unix_nanos(), 123_456_789);
    }

    #[test]
    fn from_system_time_epoch() {
        let ft = FileTime::from(UNIX_EPOCH);
        assert_eq!(ft.unix_seconds(), 0);
        assert_eq!(ft.unix_nanos(), 0);
    }

    #[test]
    fn from_system_time_now() {
        let now = SystemTime::now();
        let ft = FileTime::from(now);
        let dur = now.duration_since(UNIX_EPOCH).unwrap();
        assert_eq!(ft.unix_seconds(), dur.as_secs() as i64);
        assert_eq!(ft.unix_nanos(), dur.subsec_nanos());
    }

    #[test]
    fn from_ref_system_time() {
        let now = SystemTime::now();
        let ft = FileTime::from(&now);
        let dur = now.duration_since(UNIX_EPOCH).unwrap();
        assert_eq!(ft.unix_seconds(), dur.as_secs() as i64);
    }

    #[test]
    fn from_system_time_pre_epoch() {
        let early = UNIX_EPOCH - Duration::from_secs(3600);
        let ft = FileTime::from(early);
        // pre-epoch durations clamp to zero
        assert_eq!(ft.unix_seconds(), 0);
    }

    #[test]
    fn from_last_modification_time_matches_modified() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("t");

        std::fs::write(&file, b"x").unwrap();
        let meta = std::fs::metadata(&file).unwrap();

        let ft = FileTime::from_last_modification_time(&meta);
        let sys = meta.modified().unwrap();
        let expected = FileTime::from(sys);

        // Allow 1 s tolerance on Windows where MetadataExt isn't available
        #[cfg(not(unix))]
        {
            let diff = ft.unix_seconds().abs_diff(expected.unix_seconds());
            assert!(diff <= 1, "mtime differs by {}s", diff);
        }
        #[cfg(unix)]
        assert_eq!(ft.unix_seconds(), expected.unix_seconds());
    }

    #[test]
    fn set_and_read_file_times() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("t");
        std::fs::write(&file, b"x").unwrap();

        let target = FileTime::from_unix_time(1_700_000_000, 123_456_000);
        set_file_times(&file, target, target).unwrap();

        let meta = std::fs::metadata(&file).unwrap();
        let mtime = meta.modified().unwrap();
        let ft = FileTime::from(mtime);

        // Allow 2 s slack for filesystem rounding
        let diff = ft.unix_seconds().abs_diff(target.unix_seconds());
        assert!(diff <= 2, "mtime differs by {}s", diff);
    }

    #[test]
    fn set_file_times_nonexistent_path() {
        let ft = FileTime::zero();
        let err = set_file_times("/nonexistent/path/xyz", ft, ft).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }
}
