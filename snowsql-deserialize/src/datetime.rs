use time::format_description::FormatItem;

use crate::Error;

use crate::DeserializeFromStr;

static DATE_FORMAT: &[FormatItem<'_>] =
    time::macros::format_description!("[unix_timestamp][ignore count:1][subsecond]");

/// Impl for Date.
///
/// We need to fail if the Float contains decimals, this is a Date, not a DateTime.
///
impl DeserializeFromStr for time::Date {
    fn deserialize_from_str(s: Option<&str>) -> Result<Self, Error>
    where
        Self: Sized,
    {
        time::OffsetDateTime::deserialize_from_str(s).map(|dt| dt.date())
    }
}

/// Impl for Date.
///
/// We need to fail if the Float contains decimals, this is a Date, not a DateTime.
///
impl DeserializeFromStr for time::OffsetDateTime {
    fn deserialize_from_str(s: Option<&str>) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let s = s.ok_or(Error::UnexpectedNull)?;

        time::OffsetDateTime::parse(s, DATE_FORMAT).map_err(|err| Error::Format {
            given: s.into(),
            err: err.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use time::macros::datetime;

    #[test]
    fn deserialize_date_time() {
        assert_eq!(
            datetime!(2024-10-17 12:03:22.422528 UTC),
            time::OffsetDateTime::deserialize_from_str(Some("1729166602.422528000"))
                .expect("deserializing")
        );

        assert_eq!(
            datetime!(2024-10-18 10:54:22.912451 UTC),
            time::OffsetDateTime::deserialize_from_str(Some("1729248862.912451000"))
                .expect("deserializing")
        );

        assert_eq!(
            datetime!(2024-02-06 14:05:30.144 UTC),
            time::OffsetDateTime::deserialize_from_str(Some("1707228330 1440"))
                .expect("deserializing")
        );
    }
}
