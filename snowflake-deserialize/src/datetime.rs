use time::format_description::FormatItem;

use crate::Error;

use crate::DeserializeFromStr;

static DATE_FORMAT: &[FormatItem<'_>] =
    time::macros::format_description!("[unix_timestamp].[subsecond]");

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
    }
}

//fn f_to_date(f: f64) -> time::OffsetDateTime {
//    use time::Duration;
//
//    let dt = if f < 1. {
//        time::OffsetDateTime::UNIX_EPOCH
//    } else if f < 60. {
//        time::macros::datetime!(1899-12-31 00:00:00).assume_utc()
//    } else {
//        time::macros::datetime!(1899-12-30 00:00:00).assume_utc()
//    };
//
//    let days = f.floor();
//    let part_day = f - days;
//    let hours = (part_day * 24.0).floor();
//    let part_day = part_day * 24f64 - hours;
//    let minutes = (part_day * 60f64).floor();
//    let part_day = part_day * 60f64 - minutes;
//    let seconds = (part_day * 60f64).round();
//
//    eprintln!("days: {days}");
//    eprintln!("hours: {hours}");
//    eprintln!("minutes: {minutes}");
//    eprintln!("seconds: {seconds}");
//
//    dt + Duration::days(days as i64)
//        + Duration::hours(hours as i64)
//        + Duration::minutes(minutes as i64)
//        + Duration::seconds(seconds as i64)
//}
