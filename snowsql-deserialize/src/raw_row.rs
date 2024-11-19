use crate::{FromRow, FromRowError, RowAccess};

#[derive(Debug, Clone)]
pub struct RawRow(pub Vec<Option<String>>);

impl FromRow for RawRow {
    fn from_row<'de, A>(mut seq: RowAccess<A>) -> Result<Self, FromRowError>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut v = seq.size_hint().map(Vec::with_capacity).unwrap_or_default();

        loop {
            match seq.next::<Option<String>>("") {
                Ok(opt_s) => v.push(opt_s),
                Err(FromRowError::MissingField { .. }) => return Ok(Self(v)),

                Err(err) => return Err(err),
            }
        }
    }
}
