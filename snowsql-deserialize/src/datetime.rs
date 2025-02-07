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

        println!("parsing `{s}` as Date: {date}");
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

            // for some reason all dates received from Snowsql are marked as +1440 minutes (24h)
            // So we do modulo 24 on the offset..... This is probably wrong but I see no other solution.

            let offset = time::UtcOffset::from_whole_seconds((offset_minutes % 1440) * 60)
                .map_err(|err| Error::Value {
                    given: s.into(),
                    err: format!("invalid offset in minutes `{offset_minutes}`: {err}"),
                })?;

            Ok(utc_time.replace_offset(offset))
        } else {
            parse_utc(s)
        }
    }
}

fn parse_utc(s: &str) -> Result<time::OffsetDateTime, Error> {
    let max_seconds: i64 = (time::PrimitiveDateTime::MAX.assume_utc()
        - time::OffsetDateTime::UNIX_EPOCH)
        .whole_seconds();

    let mut parts = s.split('.');

    let secs_s = parts.next().ok_or_else(|| Error::Format {
        given: s.into(),
        err: "invalid DateTime".into(),
    })?;

    let parsed_secs = secs_s.parse::<i64>().map_err(|err| Error::Format {
        given: s.into(),
        err: format!("parsing seconds from `{secs_s}`: {err}"),
    })?;

    let secs = if max_seconds < parsed_secs {
        max_seconds
    } else {
        parsed_secs
    };

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

    let res = time::OffsetDateTime::UNIX_EPOCH
        .checked_add(time::Duration::seconds(secs))
        .ok_or_else(|| Error::Format {
            given: s.into(),
            err: format!("value to large. `{secs}` seconds"),
        })?
        .checked_add(time::Duration::nanoseconds(nanos))
        .ok_or_else(|| Error::Format {
            given: s.into(),
            err: format!("value to large. `{nanos}` nanoseconds"),
        })?;

    Ok(res)
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
            datetime!(2024-02-06 14:05:30 +0000),
            time::OffsetDateTime::from_value("1707228330 1440").expect("deserializing")
        );

        assert_eq!(
            datetime!(2024-11-14 08:59:19.273124000 +0000),
            time::OffsetDateTime::from_value("1731574759.273124000 1440").expect("deserializing")
        );
    }

    /// when encountering a date that cannot fit into a normal time.
    /// (i.e. after the year 9999) we should truncate it to 9999-12-31
    #[test]
    fn deserialize_too_large_datetime() {
        assert_eq!(
            datetime!(9999-12-31 23:59:59.0 +0000),
            time::OffsetDateTime::from_value("6318666835200 1440").expect("deserializing")
        );
    }

    #[test]
    fn deserialize_due_to_dates_being_one_off() {
        assert_eq!(
            datetime!(2022-02-25 09:16:01.900368 +0000),
            time::OffsetDateTime::from_value("1645780561.900368000 1440").expect("deserializing")
        );

        assert_eq!(
            datetime!(2022-02-25 09:22:22.957985 +0000),
            time::OffsetDateTime::from_value("1645780942.957985000 1440").expect("deserializing")
        );

        assert_eq!(
            datetime!(2022-02-03 00:00:00.000 +0000),
            time::OffsetDateTime::from_value("1643846400.000000000 1440").expect("deserializing")
        );

        assert_eq!(
            datetime!(2025-01-31 22:59:59.000 +0000),
            time::OffsetDateTime::from_value("1738364399.000000000 1440").expect("deserializing")
        );
    }
}

// EXAMPLE AGREEMENT that has times with offset 1440
/*
{
  "resultSetMetaData": {
    "numRows": 1,
    "format": "jsonv2",
    "partitionInfo": [
      {
        "rowCount": 1,
        "uncompressedSize": 410
      }
    ],
    "rowType": [
      {
        "name": "AGREEMENT_ID",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "PARKING_AGREEMENTS_PARKANDO",
        "nullable": false,
        "byteLength": 144,
        "length": 36,
        "type": "text",
        "scale": null,
        "precision": null,
        "collation": null
      },
      {
        "name": "DEBTOR_ID",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "PARKING_AGREEMENTS_PARKANDO",
        "nullable": true,
        "byteLength": 8000,
        "length": 2000,
        "type": "text",
        "scale": null,
        "precision": null,
        "collation": null
      },
      {
        "name": "SPOT_ID",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "PARKING_AGREEMENTS_PARKANDO",
        "nullable": false,
        "byteLength": 16777216,
        "length": 16777216,
        "type": "text",
        "scale": null,
        "precision": null,
        "collation": null
      },
      {
        "name": "AGREEMENT_NUMBER",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "PARKING_AGREEMENTS_PARKANDO",
        "nullable": true,
        "byteLength": null,
        "length": null,
        "type": "fixed",
        "scale": 0,
        "precision": 38,
        "collation": null
      },
      {
        "name": "IS_VAT_APPLIED",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "PARKING_AGREEMENTS_PARKANDO",
        "nullable": true,
        "byteLength": null,
        "length": null,
        "type": "boolean",
        "scale": null,
        "precision": null,
        "collation": null
      },
      {
        "name": "AGREEMENT_PRICE_EXCL_VAT",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "PARKING_AGREEMENTS_PARKANDO",
        "nullable": true,
        "byteLength": null,
        "length": null,
        "type": "fixed",
        "scale": 2,
        "precision": 10,
        "collation": null
      },
      {
        "name": "AGREEMENT_PROPOSAL_DECLINED",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "PARKING_AGREEMENTS_PARKANDO",
        "nullable": true,
        "byteLength": null,
        "length": null,
        "type": "timestamp_tz",
        "scale": 9,
        "precision": 0,
        "collation": null
      },
      {
        "name": "AGREEMENT_PROPOSAL_SENT",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "PARKING_AGREEMENTS_PARKANDO",
        "nullable": true,
        "byteLength": null,
        "length": null,
        "type": "timestamp_tz",
        "scale": 9,
        "precision": 0,
        "collation": null
      },
      {
        "name": "AGREEMENT_SIGNED",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "PARKING_AGREEMENTS_PARKANDO",
        "nullable": true,
        "byteLength": null,
        "length": null,
        "type": "timestamp_tz",
        "scale": 9,
        "precision": 0,
        "collation": null
      },
      {
        "name": "AGREEMENT_START",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "PARKING_AGREEMENTS_PARKANDO",
        "nullable": true,
        "byteLength": null,
        "length": null,
        "type": "timestamp_tz",
        "scale": 9,
        "precision": 0,
        "collation": null
      },
      {
        "name": "AGREEMENT_END",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "PARKING_AGREEMENTS_PARKANDO",
        "nullable": true,
        "byteLength": null,
        "length": null,
        "type": "timestamp_tz",
        "scale": 9,
        "precision": 0,
        "collation": null
      },
      {
        "name": "AGREEMENT_STATE",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "PARKING_AGREEMENTS_PARKANDO",
        "nullable": true,
        "byteLength": 800,
        "length": 200,
        "type": "text",
        "scale": null,
        "precision": null,
        "collation": null
      },
      {
        "name": "AGREEMENT_TYPE",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "PARKING_AGREEMENTS_PARKANDO",
        "nullable": true,
        "byteLength": 800,
        "length": 200,
        "type": "text",
        "scale": null,
        "precision": null,
        "collation": null
      },
      {
        "name": "LOCATION_ID",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "PARKING_AGREEMENTS_PARKANDO",
        "nullable": true,
        "byteLength": 144,
        "length": 36,
        "type": "text",
        "scale": null,
        "precision": null,
        "collation": null
      },
      {
        "name": "LOCATION_NAME",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "PARKING_AGREEMENTS_PARKANDO",
        "nullable": true,
        "byteLength": 16777216,
        "length": 16777216,
        "type": "text",
        "scale": null,
        "precision": null,
        "collation": null
      },
      {
        "name": "NUMBER_OF_PERMITS",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "PARKING_AGREEMENTS_PARKANDO",
        "nullable": true,
        "byteLength": null,
        "length": null,
        "type": "fixed",
        "scale": 0,
        "precision": 38,
        "collation": null
      },
      {
        "name": "PRICE_PER_PERMIT_EXCL_VAT",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "PARKING_AGREEMENTS_PARKANDO",
        "nullable": true,
        "byteLength": null,
        "length": null,
        "type": "fixed",
        "scale": 2,
        "precision": 10,
        "collation": null
      },
      {
        "name": "IS_DELETED_IN_SOURCE",
        "database": "M46_DATA_SHARE_PARKING",
        "schema": "PUBLIC",
        "table": "PARKING_AGREEMENTS_PARKANDO",
        "nullable": true,
        "byteLength": null,
        "length": null,
        "type": "boolean",
        "scale": null,
        "precision": null,
        "collation": null
      }
    ]
  },
  "data": [
    [
      "fee5c58c-27f4-4dd3-8147-e9715a32a000", // Agreement ID
      "b95ad9c1-2a69-415f-bfab-351573554142", // Debtor ID
      "63d7916f-d925-4fca-8c95-94a0552bfd7d", // Spot ID
      "132201993", // Agreement Number
      "true", // is vat applied
      "1180.14", // Agreement price EXCL vat
      null, // Propsal declined
      "1645780561.900368000 1440", // proposal sent
      "1645780942.957985000 1440", // signed at
      "1643846400.000000000 1440", // start at
      "1738364399.000000000 1440", // end at
      "Avslutat",
      "För reserverad plats",
      "dbf252a1-10b1-4c5a-8e77-330fbac4c87b",
      "Nöten 3 - Solna Strand - VASAKRONAN",
      "1",
      "1180.14",
      "false"
    ]
  ],
  "code": "090001",
  "statementStatusUrl": "/api/v2/statements/01ba3922-0103-f6f1-0000-a21900598012?requestId=e53162f1-1f6a-471c-a9f4-dce6a8448f94",
  "requestId": "e53162f1-1f6a-471c-a9f4-dce6a8448f94",
  "sqlState": "00000",
  "statementHandle": "01ba3922-0103-f6f1-0000-a21900598012",
  "message": "Statement executed successfully.",
  "createdOn": 1738892299080
}
*/
