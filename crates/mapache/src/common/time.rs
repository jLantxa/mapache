//! Date and time helpers.
//!
//! Mapache persists timestamps using chrono's RFC 3339 representation. Jiff is
//! used internally now, so the serde adapter in this module reproduces that
//! wire format byte-for-byte to keep repository IDs stable.

use std::time::SystemTime;

use jiff::{ZonedDateTime, tz::TimeZone};

use crate::common::error::{MapacheError, Result};

pub(crate) type DateTime = ZonedDateTime;
pub(crate) type Duration = jiff::SignedDuration;

/// Returns the current time in the system time zone.
pub(crate) fn now() -> DateTime {
    DateTime::now()
}

/// Returns the current time in UTC.
pub(crate) fn now_utc() -> DateTime {
    jiff::Timestamp::now().to_zoned_datetime(TimeZone::UTC)
}

/// Returns the system time zone.
pub(crate) fn system_tz() -> TimeZone {
    TimeZone::system()
}

/// Converts a `SystemTime` to the system time zone.
pub(crate) fn from_system_time(time: SystemTime) -> Result<DateTime> {
    DateTime::try_from(time).map_err(Into::into)
}

/// Creates a datetime from whole seconds since the Unix epoch in the system
/// time zone.
#[cfg(test)]
pub(crate) fn from_unix_timestamp(seconds: i64) -> Result<DateTime> {
    Ok(jiff::Timestamp::from_second(seconds)?.to_zoned_datetime(system_tz()))
}

/// Converts a standard duration, returning an error when it exceeds jiff's
/// signed duration range.
pub(crate) fn duration_from_std(duration: std::time::Duration) -> Result<Duration> {
    Duration::try_from(duration).map_err(Into::into)
}

/// Converts a signed duration to a standard duration. Negative durations are
/// rejected, matching `chrono::Duration::to_std`.
pub(crate) fn duration_to_std(duration: Duration) -> Result<std::time::Duration> {
    std::time::Duration::try_from(duration).map_err(Into::into)
}

/// Parses a whole-millisecond duration using the same range as chrono's
/// `Duration` (±i64::MAX milliseconds).
fn chrono_duration_from_millis(millis: i128) -> Result<Duration> {
    if millis < i128::from(i64::MIN) || millis > i128::from(i64::MAX) {
        return Err(MapacheError::Format(
            "duration value out of range".to_string(),
        ));
    }
    #[expect(clippy::cast_possible_truncation, reason = "range checked above")]
    Ok(Duration::from_millis(millis as i64))
}

/// Parses a duration string (e.g., "1d", "2w", "3m", "4y", "5h", "6s").
///
/// Supported units are seconds, minutes, hours, days, weeks and years. Years
/// are approximated as 365 days, preserving the previous CLI behavior.
pub(crate) fn parse_duration_string(s: &str) -> Result<Duration> {
    if s.is_empty() {
        return Err(MapacheError::Format(
            "invalid duration format: empty string".to_string(),
        ));
    }

    let mut total_millis: i128 = 0;
    let mut current_num_str = String::new();

    for c in s.chars() {
        if c.is_ascii_digit() {
            current_num_str.push(c);
            continue;
        }

        if current_num_str.is_empty() {
            return Err(MapacheError::Format(format!(
                "invalid duration format: unit '{c}' without preceding number in \"{s}\""
            )));
        }

        let num = current_num_str.parse::<i64>().map_err(|e| {
            MapacheError::Format(format!(
                "failed to parse number before unit '{c}' in \"{s}\": {e}"
            ))
        })?;

        let millis_per_unit: i128 = match c {
            's' => 1_000,
            'm' => 60_000,
            'h' => 3_600_000,
            'd' => 86_400_000,
            'w' => 604_800_000,
            'y' => 31_536_000_000,
            _ => {
                return Err(MapacheError::Format(format!(
                    "invalid duration unit: '{c}' in \"{s}\""
                )));
            }
        };

        let part = i128::from(num)
            .checked_mul(millis_per_unit)
            .ok_or_else(|| {
                MapacheError::Format(format!(
                    "duration value out of range for '{num}{c}' in \"{s}\""
                ))
            })?;
        if part < i128::from(i64::MIN) || part > i128::from(i64::MAX) {
            return Err(MapacheError::Format(format!(
                "duration value out of range for '{num}{c}' in \"{s}\""
            )));
        }

        total_millis = total_millis.checked_add(part).ok_or_else(|| {
            MapacheError::Format(format!(
                "duration value out of range when combining units in \"{s}\""
            ))
        })?;
        if total_millis < i128::from(i64::MIN) || total_millis > i128::from(i64::MAX) {
            return Err(MapacheError::Format(format!(
                "duration value out of range when combining units in \"{s}\""
            )));
        }
        current_num_str.clear();
    }

    if !current_num_str.is_empty() {
        return Err(MapacheError::Format(format!(
            "invalid duration format: trailing number '{current_num_str}' without unit in \"{s}\""
        )));
    }

    chrono_duration_from_millis(total_millis)
}

/// Serde adapter that reproduces chrono's default `DateTime<Local>` encoding:
/// RFC 3339 with automatic 0/3/6/9 digit fractional seconds and `Z` for UTC.
pub(crate) mod serde {
    use std::str::FromStr;

    use jiff::{Timestamp, fmt::temporal::DateTimePrinter, tz::Offset};
    use serde::{Deserialize, Deserializer, Serializer};

    use crate::common::time::{DateTime, system_tz};

    pub(crate) fn serialize<S>(
        value: &DateTime,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format_chrono_rfc3339(value))
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> std::result::Result<DateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        let timestamp = Timestamp::from_str(&value).map_err(::serde::de::Error::custom)?;
        Ok(timestamp.to_zoned_datetime(system_tz()))
    }

    pub(super) fn format_chrono_rfc3339(value: &DateTime) -> String {
        static PRINTER_SECONDS: DateTimePrinter = DateTimePrinter::new().precision(Some(0));
        static PRINTER_MILLIS: DateTimePrinter = DateTimePrinter::new().precision(Some(3));
        static PRINTER_MICROS: DateTimePrinter = DateTimePrinter::new().precision(Some(6));
        static PRINTER_NANOS: DateTimePrinter = DateTimePrinter::new().precision(Some(9));

        let dt = value.datetime();
        // Jiff and chrono disagree on how to spell years outside `0000..=9999`,
        // so keep the old spelling for those rare timestamps.
        if !(0..=9999).contains(&dt.year()) {
            return format_out_of_range_year(value);
        }

        // Chrono uses a fixed 0/3/6/9 digits, while jiff's automatic precision
        // strips trailing zeroes. Select the precision explicitly.
        let nanos = value.subsec_nanosecond();
        let printer = if nanos == 0 {
            &PRINTER_SECONDS
        } else if nanos % 1_000_000 == 0 {
            &PRINTER_MILLIS
        } else if nanos % 1_000 == 0 {
            &PRINTER_MICROS
        } else {
            &PRINTER_NANOS
        };

        let mut out = String::new();
        let result = if value.offset() == Offset::UTC {
            printer.print_timestamp(&value.timestamp(), &mut out)
        } else {
            printer.print_timestamp_with_offset(&value.timestamp(), value.offset(), &mut out)
        };
        // Printing to a `String` can only fail if writing fails.
        result.expect("writing a timestamp to a String cannot fail");
        out
    }

    fn format_out_of_range_year(value: &DateTime) -> String {
        let dt = value.datetime();
        let nanos = value.subsec_nanosecond();
        let fraction = if nanos == 0 {
            String::new()
        } else if nanos % 1_000_000 == 0 {
            format!(".{:03}", nanos / 1_000_000)
        } else if nanos % 1_000 == 0 {
            format!(".{:06}", nanos / 1_000)
        } else {
            format!(".{nanos:09}")
        };

        let offset_seconds = value.offset().seconds();
        let offset = if offset_seconds == 0 {
            "Z".to_string()
        } else {
            let sign = if offset_seconds < 0 { '-' } else { '+' };
            let absolute = offset_seconds.unsigned_abs();
            // chrono's RFC 3339 writer rounds the offset to the nearest
            // minute (half up) after taking its absolute value.
            let total_minutes = (absolute + 30) / 60;
            let hours = total_minutes / 60;
            let minutes = total_minutes % 60;
            format!("{sign}{hours:02}:{minutes:02}")
        };

        format!(
            "{:+05}-{:02}-{:02}T{:02}:{:02}:{:02}{fraction}{offset}",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second()
        )
    }
}

/// Serde adapter for optional timestamps using the same wire format as
/// [`serde`].
pub(crate) mod serde_option {
    use std::str::FromStr;

    use jiff::Timestamp;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::common::time::{DateTime, serde as time_serde, system_tz};

    struct BorrowedDateTime<'a>(&'a DateTime);

    impl Serialize for BorrowedDateTime<'_> {
        fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_str(&time_serde::format_chrono_rfc3339(self.0))
        }
    }

    pub(crate) fn serialize<S>(
        value: &Option<DateTime>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(value) => serializer.serialize_some(&BorrowedDateTime(value)),
            None => serializer.serialize_none(),
        }
    }

    pub(crate) fn deserialize<'de, D>(
        deserializer: D,
    ) -> std::result::Result<Option<DateTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<String>::deserialize(deserializer)?;
        value
            .map(|value| {
                let timestamp = Timestamp::from_str(&value).map_err(::serde::de::Error::custom)?;
                Ok(timestamp.to_zoned_datetime(system_tz()))
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use jiff::{
        civil::date,
        tz::{Offset, TimeZone},
    };
    use serde::{Deserialize, Serialize};

    use crate::common::time::{self, DateTime, Duration, parse_duration_string};

    #[derive(Serialize, Deserialize)]
    struct Wrapper {
        #[serde(with = "time::serde")]
        timestamp: DateTime,
    }

    #[derive(Serialize, Deserialize)]
    struct OptionalWrapper {
        #[serde(default, with = "time::serde_option")]
        timestamp: Option<DateTime>,
    }

    fn fixed_datetime(nanos: i32) -> DateTime {
        date(2025, 11, 20)
            .at(19, 0, 0, nanos)
            .to_zoned_datetime(jiff::tz::offset(1).to_time_zone())
            .expect("valid fixed datetime")
    }

    #[test]
    fn serde_matches_chrono_rfc3339_precision() {
        let cases = [
            (0, "2025-11-20T19:00:00+01:00"),
            (123_000_000, "2025-11-20T19:00:00.123+01:00"),
            (123_456_000, "2025-11-20T19:00:00.123456+01:00"),
            (375_479_500, "2025-11-20T19:00:00.375479500+01:00"),
        ];

        for (nanos, expected) in cases {
            let json = serde_json::to_string(&Wrapper {
                timestamp: fixed_datetime(nanos),
            })
            .expect("serialize");
            assert_eq!(json, format!(r#"{{"timestamp":"{expected}"}}"#));
        }
    }

    #[test]
    fn serde_uses_z_for_utc() {
        let datetime = date(2025, 11, 20)
            .at(19, 0, 0, 0)
            .to_zoned_datetime(TimeZone::UTC)
            .expect("valid UTC datetime");
        let json = serde_json::to_string(&Wrapper {
            timestamp: datetime,
        })
        .expect("serialize");
        assert_eq!(json, r#"{"timestamp":"2025-11-20T19:00:00Z"}"#);
    }

    #[test]
    fn serde_rounds_offset_to_nearest_minute_like_chrono() {
        let datetime = date(2025, 11, 20)
            .at(19, 0, 0, 0)
            .to_zoned_datetime(Offset::from_seconds(30).unwrap().to_time_zone())
            .expect("valid datetime");
        let json = serde_json::to_string(&Wrapper {
            timestamp: datetime,
        })
        .expect("serialize");
        assert_eq!(json, r#"{"timestamp":"2025-11-20T19:00:00+00:01"}"#);
    }

    #[test]
    fn serde_option_roundtrips_none_and_some() {
        let none = OptionalWrapper { timestamp: None };
        assert_eq!(
            serde_json::to_string(&none).expect("serialize"),
            r#"{"timestamp":null}"#
        );

        let some = OptionalWrapper {
            timestamp: Some(
                date(2025, 11, 20)
                    .at(19, 0, 0, 0)
                    .to_zoned_datetime(TimeZone::UTC)
                    .expect("valid UTC datetime"),
            ),
        };
        let json = serde_json::to_string(&some).expect("serialize");
        assert_eq!(json, r#"{"timestamp":"2025-11-20T19:00:00Z"}"#);
        let parsed: OptionalWrapper = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(
            parsed.timestamp.expect("timestamp").timestamp(),
            some.timestamp.expect("timestamp").timestamp()
        );
    }

    #[test]
    fn serde_parses_legacy_timestamps_as_the_same_instant() {
        let wrapper: Wrapper =
            serde_json::from_str(r#"{"timestamp":"2025-11-20T19:00:00.375479500+01:00"}"#)
                .expect("deserialize");
        let expected = fixed_datetime(375_479_500).timestamp();
        assert_eq!(wrapper.timestamp.timestamp(), expected);
    }

    #[test]
    fn duration_parser_preserves_year_and_bounds_behavior() {
        assert_eq!(
            parse_duration_string("1y").expect("one year"),
            Duration::from_secs(365 * 86_400)
        );
        assert!(parse_duration_string(&format!("{}d", i64::MAX)).is_err());
    }
}
