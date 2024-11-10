use std::str::FromStr;

pub mod bindings;

mod error;

pub use error::Error;

#[cfg(feature = "time")]
mod datetime;

pub use bindings::*;

pub type Result<T> = std::result::Result<T, Error>;

pub trait FromRow {
    fn from_row(row: Vec<Option<String>>) -> Result<Self>
    where
        Self: Sized;
}

/// For custom data parsing,
/// ex. you want to convert the retrieved data (strings) to enums.
///
/// Data in cells are not their type, they are simply strings that need to be converted.
pub trait DeserializeFromStr {
    fn deserialize_from_str(s: Option<&str>) -> Result<Self>
    where
        Self: Sized;
}

impl<T> DeserializeFromStr for Option<T>
where
    T: DeserializeFromStr,
{
    fn deserialize_from_str(s: Option<&str>) -> Result<Self> {
        if s.is_none() {
            Ok(None)
        } else {
            T::deserialize_from_str(s).map(Some)
        }
    }
}
macro_rules! impl_deserialize_from_str {
    ($ty: ty) => {
        impl DeserializeFromStr for $ty {
            fn deserialize_from_str(s: Option<&str>) -> Result<Self> {
                s.ok_or(Error::UnexpectedNull).and_then(|s| {
                    <$ty>::from_str(s).map_err(|err| Error::Format {
                        given: s.into(),
                        err: err.to_string(),
                    })
                })
            }
        }
    };
}

impl_deserialize_from_str!(bool);
impl_deserialize_from_str!(usize);
impl_deserialize_from_str!(isize);
impl_deserialize_from_str!(u8);
impl_deserialize_from_str!(u16);
impl_deserialize_from_str!(u32);
impl_deserialize_from_str!(u64);
impl_deserialize_from_str!(u128);
impl_deserialize_from_str!(i16);
impl_deserialize_from_str!(i32);
impl_deserialize_from_str!(i64);
impl_deserialize_from_str!(i128);
impl_deserialize_from_str!(f32);
impl_deserialize_from_str!(f64);
impl_deserialize_from_str!(String);
impl_deserialize_from_str!(uuid::Uuid);
