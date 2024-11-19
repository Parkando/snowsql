use std::{borrow::Cow, marker::PhantomData};

use crate::{FromRowError, FromValue};

pub trait FromRow
where
    Self: Sized,
{
    fn from_row<'de, A>(seq: RowAccess<A>) -> Result<Self, FromRowError>
    where
        A: serde::de::SeqAccess<'de>;
}
#[derive(Debug)]
pub struct Row<T>(pub T);

pub struct RowAccess<A> {
    idx: usize,
    seq: A,
}
impl<'de, A> RowAccess<A>
where
    A: serde::de::SeqAccess<'de>,
{
    pub fn new(seq: A) -> Self {
        Self { idx: 0, seq }
    }

    pub fn next<T>(&mut self, field: &'static str) -> Result<T, FromRowError>
    where
        T: FromValue,
    {
        let res = self
            .seq
            .next_element::<Option<Cow<'de, str>>>()
            .map_err(|err| FromRowError::NonStrField {
                field,
                err: err.to_string(),
            })?
            .ok_or_else(|| FromRowError::MissingField {
                idx: self.idx,
                field,
            })?;

        self.idx += 1;

        T::from_optional_value(res.as_deref())
            .map_err(|err| FromRowError::DeserializingField { field, err })
    }

    pub fn size_hint(&self) -> Option<usize> {
        self.seq.size_hint()
    }
}

impl<'de, T> serde::Deserialize<'de> for Row<T>
where
    T: FromRow,
{
    fn deserialize<D>(des: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        des.deserialize_seq(Visitor(PhantomData))
    }
}

struct Visitor<T>(PhantomData<T>);

impl<'de, T> serde::de::Visitor<'de> for Visitor<T>
where
    T: FromRow,
{
    type Value = Row<T>;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a snowflake row")
    }

    fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        T::from_row(RowAccess::new(seq))
            .map_err(|err| serde::de::Error::custom(err.to_string()))
            .map(Row)
    }
}
