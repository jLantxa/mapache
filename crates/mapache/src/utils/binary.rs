use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::common::error::{MapacheError, Result};

pub(crate) fn put_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}

pub(crate) fn get_u8(buf: &mut &[u8]) -> Result<u8> {
    let arr = get_array::<1>(buf)?;
    Ok(arr[0])
}

pub(crate) fn put_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn get_u16(buf: &mut &[u8]) -> Result<u16> {
    let arr = get_array::<2>(buf)?;
    Ok(u16::from_le_bytes(arr))
}

pub(crate) fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn get_u32(buf: &mut &[u8]) -> Result<u32> {
    let arr = get_array::<4>(buf)?;
    Ok(u32::from_le_bytes(arr))
}

pub(crate) fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn get_u64(buf: &mut &[u8]) -> Result<u64> {
    let arr = get_array::<8>(buf)?;
    Ok(u64::from_le_bytes(arr))
}

pub(crate) fn put_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn get_i64(buf: &mut &[u8]) -> Result<i64> {
    let arr = get_array::<8>(buf)?;
    Ok(i64::from_le_bytes(arr))
}

pub(crate) fn put_bytes(buf: &mut Vec<u8>, v: &[u8]) {
    buf.extend_from_slice(v);
}

pub(crate) fn get_exact<'a>(buf: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    if buf.len() < len {
        return Err(MapacheError::Format(format!(
            "unexpected end of input: needed {len} bytes, have {}",
            buf.len()
        )));
    }
    let (val, rest) = buf.split_at(len);
    *buf = rest;
    Ok(val)
}

pub(crate) fn get_array<const N: usize>(buf: &mut &[u8]) -> Result<[u8; N]> {
    let slice = get_exact(buf, N)?;
    Ok(slice
        .try_into()
        .expect("get_exact returns a slice of exactly N bytes"))
}

pub(crate) fn put_str(buf: &mut Vec<u8>, v: &str) {
    put_u32(buf, v.len() as u32);
    buf.extend_from_slice(v.as_bytes());
}

pub(crate) fn get_string(buf: &mut &[u8]) -> Result<String> {
    let len = get_u32(buf)? as usize;
    let bytes = get_exact(buf, len)?;
    String::from_utf8(bytes.to_vec())
        .map_err(|e| MapacheError::Format(format!("invalid UTF-8: {e}")))
}

pub(crate) fn put_time(buf: &mut Vec<u8>, time: &SystemTime) {
    match time.duration_since(UNIX_EPOCH) {
        Ok(d) => {
            put_i64(buf, d.as_secs() as i64);
            put_u32(buf, d.subsec_nanos());
        }
        Err(e) => {
            let d = e.duration();
            put_i64(buf, -(d.as_secs() as i64));
            put_u32(buf, d.subsec_nanos());
        }
    }
}

pub(crate) fn get_time(buf: &mut &[u8]) -> Result<SystemTime> {
    let secs = get_i64(buf)?;
    let nanos = get_u32(buf)?;
    if secs >= 0 {
        Ok(UNIX_EPOCH + Duration::new(secs as u64, nanos))
    } else {
        Ok(UNIX_EPOCH - Duration::new((-secs) as u64, nanos))
    }
}
