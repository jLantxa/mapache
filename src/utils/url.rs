// [backup] is an incremental backup tool
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use anyhow::Result;

use std::str::FromStr;

/// Represents a parsed URL structure.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Url {
    pub scheme: String,
    pub username: String,
    pub password: Option<String>,
    pub host: Option<String>, // Can be null (e.g., for `file:///path`)
    pub port: Option<u16>,
    pub path: Vec<String>, // Stored as decoded segments, e.g., ["a", "b"] for "/a/b"
    pub query: Option<String>, // Stored as raw query string (not parsed into key-value pairs)
    pub fragment: Option<String>, // Stored as raw fragment string
}

/// Represents errors that can occur during URL parsing.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum UrlError {
    InvalidScheme,
    InvalidHost,
    InvalidPort,
    PercentDecodingError,
}

impl std::fmt::Display for UrlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UrlError::InvalidScheme => write!(f, "Invalid URL scheme"),
            UrlError::InvalidHost => write!(f, "Invalid host in URL"),
            UrlError::InvalidPort => write!(f, "Invalid port number in URL"),
            UrlError::PercentDecodingError => write!(f, "Percent decoding error in URL component"),
        }
    }
}

impl std::error::Error for UrlError {}

/// A basic percent decoding function.
/// Decodes `%xx` sequences into their corresponding bytes and then attempts to
/// interpret the resulting byte sequence as UTF-8.
///
/// This is a simplified version of a full standard-compliant percent decoder.
/// It doesn't handle all edge cases or specific character sets for different URL components.
fn percent_decode(input: &str) -> Result<String, UrlError> {
    let mut bytes = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '%' {
            let hex_digit1_char = chars.next().ok_or(UrlError::PercentDecodingError)?;
            let hex_digit2_char = chars.next().ok_or(UrlError::PercentDecodingError)?;

            let mut hex_str = String::with_capacity(2);
            hex_str.push(hex_digit1_char);
            hex_str.push(hex_digit2_char);

            let byte =
                u8::from_str_radix(&hex_str, 16).map_err(|_| UrlError::PercentDecodingError)?;
            bytes.push(byte);
        } else {
            // Push valid UTF-8 characters directly
            bytes.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes());
        }
    }

    String::from_utf8(bytes).map_err(|_| UrlError::PercentDecodingError)
}

impl FromStr for Url {
    type Err = UrlError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut input_slice = s.trim(); // WHATWG standard mandates trimming C0 controls and spaces

        let scheme;
        let mut username = String::new();
        let mut password = None;
        let host: Option<String>; // Initialize host as None
        let mut port = None;

        let mut query = None;
        let mut fragment = None;

        if let Some(colon_idx) = input_slice.find(':') {
            let potential_scheme = &input_slice[..colon_idx];

            // Validate scheme: Must start with an ASCII letter, followed by
            // ASCII alphanumeric, '+', '-', or '.'
            if !potential_scheme.is_empty()
                && potential_scheme
                    .chars()
                    .next()
                    .unwrap()
                    .is_ascii_alphabetic()
                && potential_scheme
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.')
            {
                scheme = potential_scheme.to_ascii_lowercase(); // Scheme must be lowercased
                let after_scheme_colon = &input_slice[colon_idx + 1..];

                // Check for "://" which indicates a network-path URL (e.g., http://, https://)
                if after_scheme_colon.starts_with("//") {
                    input_slice = &input_slice[colon_idx + 3..]; // Consume "://"
                } else {
                    // This is an opaque path URL (e.g., mailto:user@example.com, data:text/plain,foo)
                    // For opaque paths, the remainder of the string after the scheme and colon
                    // is treated as a single path segment. It cannot have host, port, query, or fragment.
                    return Ok(Url {
                        scheme,
                        username: "".to_string(), // Opaque URLs typically don't have userinfo
                        password: None,
                        host: None,
                        port: None,
                        path: vec![percent_decode(after_scheme_colon)?], // The entire remainder is the opaque path
                        query: None,
                        fragment: None,
                    });
                }
            } else {
                // If it looks like a colon but the scheme itself is invalid, or no scheme.
                // This parser enforces a valid scheme for absolute URLs.
                return Err(UrlError::InvalidScheme);
            }
        } else {
            // No scheme found. This parser requires a scheme to be present.
            return Err(UrlError::InvalidScheme);
        }

        // --- Parsing for Network-Path URLs (after scheme and "://") ---
        // `input_slice` now contains authority (optional) + path + query (optional) + fragment (optional)

        let mut current_parsing_slice = input_slice;

        // Handle `file://` scheme specific parsing:
        // If it's a file scheme and the current_parsing_slice *doesn't* start with '/',
        // then what would normally be treated as host is actually the first path segment.
        if scheme == "file" {
            if current_parsing_slice.starts_with('/') {
                // `file:///path` or `file:////path` (leading multiple slashes become single root)
                // The host for `file:///` is implicitly null.
                host = None;
            } else {
                // `file://dir/file` or `file://relative/path`
                // Here, "dir" or "relative" is *not* a host. It's the first path segment.
                host = None; // Explicitly set host to None for these cases.
            }
        } else {
            // For other schemes, parse authority as usual.
            // Find the boundary between authority and path/query/fragment
            let mut authority_end_idx = current_parsing_slice.len();
            if let Some(i) = current_parsing_slice.find(&['/', '?', '#'][..]) {
                authority_end_idx = i;
            }
            let authority_part = &current_parsing_slice[..authority_end_idx];
            current_parsing_slice = &current_parsing_slice[authority_end_idx..]; // Remainder is now path + query + fragment

            let mut current_authority = authority_part;

            // Userinfo (username:password@)
            if let Some(at_idx) = current_authority.find('@') {
                let userinfo = &current_authority[..at_idx];
                current_authority = &current_authority[at_idx + 1..];

                if let Some(colon_idx) = userinfo.find(':') {
                    username = percent_decode(&userinfo[..colon_idx])?;
                    password = Some(percent_decode(&userinfo[colon_idx + 1..])?);
                } else {
                    username = percent_decode(userinfo)?;
                }
            }

            // Host and Port parsing from `current_authority`
            let mut host_str_candidate = current_authority;
            let mut potential_port_str = None;

            // Handle IPv6 host: `[::1]:8080`
            if host_str_candidate.starts_with('[') {
                if let Some(closing_bracket) = host_str_candidate.find(']') {
                    let ipv6_host = &host_str_candidate[1..closing_bracket];
                    if ipv6_host.is_empty() {
                        return Err(UrlError::InvalidHost); // Empty IPv6 literal (e.g., `[]`)
                    }
                    host = Some(ipv6_host.to_string());
                    if host_str_candidate.len() > closing_bracket + 1 {
                        if &host_str_candidate[closing_bracket + 1..closing_bracket + 2] == ":" {
                            potential_port_str = Some(&host_str_candidate[closing_bracket + 2..]);
                        } else {
                            return Err(UrlError::InvalidHost); // Malformed IPv6 host with extra characters
                        }
                    }
                } else {
                    return Err(UrlError::InvalidHost); // Missing closing ']' for IPv6 literal
                }
            } else {
                // Regular host (domain name or IPv4)
                // Find the last colon, assuming it's for the port
                if let Some(colon_idx) = host_str_candidate.rfind(':') {
                    let potential_port_slice = &host_str_candidate[colon_idx + 1..];
                    // Check if it's actually a port by ensuring it consists only of digits
                    if potential_port_slice.chars().all(|c| c.is_ascii_digit()) {
                        potential_port_str = Some(potential_port_slice);
                        host_str_candidate = &host_str_candidate[..colon_idx]; // Host is before the last colon
                    }
                    // If not all digits, the colon is considered part of the host (e.g., `hostname:abc`)
                    // or indicates an invalid format. For this parser, we include it in the host.
                }
                host = Some(host_str_candidate.to_ascii_lowercase()); // Hostnames should be lowercased
            }

            // Port parsing
            if let Some(p_str) = potential_port_str {
                port = Some(p_str.parse().map_err(|_| UrlError::InvalidPort)?);
            }
        }

        // --- Path, Query, Fragment parsing ---
        // `current_parsing_slice` now contains path + query + fragment (after authority for non-file schemes)

        // Fragment parsing: everything after '#'
        if let Some(hash_idx) = current_parsing_slice.find('#') {
            fragment = Some(current_parsing_slice[hash_idx + 1..].to_string());
            current_parsing_slice = &current_parsing_slice[..hash_idx];
        }

        // Query parsing: everything after '?' (and before '#')
        if let Some(query_idx) = current_parsing_slice.find('?') {
            query = Some(current_parsing_slice[query_idx + 1..].to_string());
            current_parsing_slice = &current_parsing_slice[..query_idx];
        }

        // Path parsing: everything before '?' (and before '#')
        let mut segments_to_process: Vec<String> = Vec::new();
        let path_str_for_split = if scheme == "file" {
            // For file URLs like `file://dir/file`, the "dir" part is part of the path.
            // current_parsing_slice holds the *entire* remaining part that should be path.
            // No leading slash to remove here if it wasn't there initially.
            current_parsing_slice
        } else if let Some(stripped) = current_parsing_slice.strip_prefix('/') {
            // For network-path URLs (http, https, or file:///),
            // a leading slash indicates the root but is not stored as an empty segment
            // in the path list, unless it's a `//` sequence (empty segment).
            stripped
        } else {
            current_parsing_slice
        };

        if !path_str_for_split.is_empty() {
            for raw_segment in path_str_for_split.split('/') {
                if raw_segment == "." {
                    // Ignore current directory segment
                } else if raw_segment == ".." {
                    // Pop the last segment (if any) to go up one directory
                    segments_to_process.pop();
                } else {
                    segments_to_process.push(percent_decode(raw_segment)?);
                }
            }
        }
        let path: Vec<String> = segments_to_process;

        Ok(Url {
            scheme,
            username,
            password,
            host,
            port,
            path,
            query,
            fragment,
        })
    }
}
