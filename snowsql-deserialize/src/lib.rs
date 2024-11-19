use std::str::FromStr;

pub mod bindings;

mod error;
mod raw_row;
mod row;

pub use {
    error::{Error, FromRowError},
    raw_row::RawRow,
    row::{FromRow, Row, RowAccess},
};

#[cfg(feature = "time")]
mod datetime;

pub use bindings::*;

pub type Result<T> = std::result::Result<T, Error>;
pub type FromRowResult<T> = std::result::Result<T, FromRowError>;

/// For custom data parsing,
/// ex. you want to convert the retrieved data (strings) to enums.
///
/// Data in cells are not their type, they are simply strings that need to be converted.
pub trait FromValue
where
    Self: Sized,
{
    fn from_value(s: &str) -> Result<Self>;

    fn from_optional_value(s: Option<&str>) -> Result<Self> {
        s.ok_or(Error::UnexpectedNull).and_then(Self::from_value)
    }
}

impl<T> FromValue for Option<T>
where
    T: FromValue,
{
    fn from_value(s: &str) -> Result<Self> {
        T::from_value(s).map(Some)
    }

    fn from_optional_value(s: Option<&str>) -> Result<Self> {
        s.map(T::from_value).transpose()
    }
}
macro_rules! impl_from_value {
    ($ty: ty) => {
        impl FromValue for $ty {
            fn from_value(s: &str) -> Result<Self> {
                <$ty>::from_str(s).map_err(|err| Error::Format {
                    given: s.into(),
                    err: err.to_string(),
                })
            }
        }
    };
}

impl_from_value!(bool);
impl_from_value!(usize);
impl_from_value!(isize);
impl_from_value!(u8);
impl_from_value!(u16);
impl_from_value!(u32);
impl_from_value!(u64);
impl_from_value!(u128);
impl_from_value!(i16);
impl_from_value!(i32);
impl_from_value!(i64);
impl_from_value!(i128);
impl_from_value!(f32);
impl_from_value!(f64);
impl_from_value!(String);
impl_from_value!(uuid::Uuid);
