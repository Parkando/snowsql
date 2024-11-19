use crate::{Error, FromValue};

// static DATETIME_TZ_FORMAT: &[FormatItem<'_>] =
//     time::macros::format_description!("[unix_timestamp][ignore count:1][optional [subsecond]]");

// DATE
//     Integer value (in a string) of the number of days since the epoch (e.g. 18262).

/// Impl for Date.
///
/// We need to fail if the Float contains decimals, this is a Date, not a DateTime.
///
impl FromValue for time::Date {
    fn from_value(s: &str) -> Result<Self, Error>
    where
        Self: Sized,
    {
        // days since epoch.

        let days = s.parse::<i64>().map_err(|_| Error::Format {
            given: s.into(),
            err: "time::Date expects a string with the number of days since epoch.".into(),
        })?;

        let date = time::OffsetDateTime::UNIX_EPOCH.date() + time::Duration::days(days);

        Ok(date)
    }
}

///
/// TIME, TIMESTAMP_LTZ, TIMESTAMP_NTZ
///     Float value (with 9 decimal places) of the number of seconds since the epoch (e.g. 82919.000000000).
///
///
/// TIMESTAMP_TZ
///     Float value (with 9 decimal places) of the number of seconds since the epoch,
///     followed by a space and the time zone offset in minutes (e.g. 1616173619000000000 960)
///
impl FromValue for time::OffsetDateTime {
    fn from_value(s: &str) -> Result<Self, Error>
    where
        Self: Sized,
    {
        // If TIMEZONE is missing, we expect the Date to be UTC.

        // if the string contains a space. The timezone offset (in minutes)
        // follows..
        if s.contains(' ') {
            // TIMESTAMP_TZ
            //     Float value (with 9 decimal places) of the number of seconds since the epoch,
            //     followed by a space and the time zone offset in minutes (e.g. 1616173619000000000 960)
            //

            let (dt_part, offset_part) = s.split_once(' ').unwrap();

            let utc_time = parse_utc(dt_part)?;

            let offset_minutes = offset_part.parse::<i32>().map_err(|err| Error::Format {
                given: s.into(),
                err: format!("parsing Offset minutes from `{offset_part}`: {err}"),
            })?;

            let offset =
                time::UtcOffset::from_whole_seconds(offset_minutes * 60).map_err(|err| {
                    Error::Value {
                        given: s.into(),
                        err: format!("invalid offset in minutes `{offset_minutes}`: {err}"),
                    }
                })?;

            Ok(utc_time.replace_offset(offset))
        } else {
            parse_utc(s)
        }
    }
}

fn parse_utc(s: &str) -> Result<time::OffsetDateTime, Error> {
    let mut parts = s.split('.');

    let secs_s = parts.next().ok_or_else(|| Error::Format {
        given: s.into(),
        err: "invalid DateTime".into(),
    })?;

    let secs = secs_s.parse::<i64>().map_err(|err| Error::Format {
        given: s.into(),
        err: format!("parsing seconds from `{secs_s}`: {err}"),
    })?;

    let nanos = parts
        .next()
        .map(|subsecs_s| {
            subsecs_s.parse::<i64>().map_err(|err| Error::Format {
                given: s.into(),
                err: format!("parsing subseconds from `{subsecs_s}`: {err}"),
            })
        })
        .transpose()?
        .unwrap_or(0);

    Ok(time::OffsetDateTime::UNIX_EPOCH
        + time::Duration::seconds(secs)
        + time::Duration::nanoseconds(nanos))
}

#[cfg(test)]
mod tests {

    use super::*;
    use time::macros::datetime;

    #[test]
    fn deserialize_date_time() {
        assert_eq!(
            datetime!(2024-10-17 12:03:22.422528 UTC),
            time::OffsetDateTime::from_value("1729166602.422528000").expect("deserializing")
        );

        assert_eq!(
            datetime!(2024-10-18 10:54:22.912451 UTC),
            time::OffsetDateTime::from_value("1729248862.912451000").expect("deserializing")
        );

        assert_eq!(
            datetime!(2024-02-06 14:05:30 +24),
            time::OffsetDateTime::from_value("1707228330 1440").expect("deserializing")
        );

        assert_eq!(
            datetime!(2024-11-14 08:59:19.273124000 +24),
            time::OffsetDateTime::from_value("1731574759.273124000 1440").expect("deserializing")
        );
    }
}
