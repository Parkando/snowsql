use snowsql_deserialize::{FromRow, RawRow};

use crate::QueryBuilder;

#[allow(async_fn_in_trait)]
pub trait Selectable
where
    Self: Sized,
    Self: FromRow,
{
    const TABLE_NAME: &str;
    const SELECT: &str;
    const ORDER_BY: &str;

    /// Starts a query by selecting all fields in the Selectable struct
    /// from the provided table_source
    fn select_all() -> QueryBuilder<Self> {
        crate::sql(format!("SELECT {} FROM {}", Self::SELECT, Self::TABLE_NAME,))
            .order_by(Self::ORDER_BY)
    }

    async fn query_count(c: &crate::Client) -> crate::Result<u64> {
        let res = crate::sql::<RawRow>(format!("SELECT count(*) FROM {}", Self::TABLE_NAME))
            .query(c)
            .await?;

        let col_str = res
            .data
            .first()
            .and_then(|row| {
                row.0
                     .0
                    .first()
                    .map(|s| s.as_ref().expect("one col").as_str())
            })
            .ok_or_else(|| {
                snowsql_deserialize::Error::Other(
                    "expected a single row and column back from count(*) query".into(),
                )
            })?;

        let count = col_str.parse::<u64>().map_err(|_| {
            snowsql_deserialize::Error::Other("could not parse col count as u64".into())
        })?;

        Ok(count)
    }
}
