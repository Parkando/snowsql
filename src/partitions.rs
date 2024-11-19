use std::marker::PhantomData;

use snowsql_deserialize::{FromRow, Row};

use crate::{Client, ResponseOk, Result};

pub struct Partitions<R> {
    pub info: super::ResponseInfo,
    first_res: Option<Vec<Row<R>>>,
    next_index: usize,
    partition_count: usize,

    _marker: PhantomData<R>,
}

#[derive(Debug)]
pub struct Partition<R> {
    pub data: Vec<R>,
    pub index: usize,
    pub total_count: usize,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct RawPartition<R> {
    data: Vec<R>,
}

impl<R> Partitions<R>
where
    R: FromRow,
{
    pub(crate) fn from_response(r: super::Response<Row<R>>) -> Self {
        let partition_count = r.info.meta.partition_info.len();
        Self {
            info: r.info,
            first_res: Some(r.data),
            next_index: 0,
            partition_count,

            _marker: PhantomData,
        }
    }

    pub async fn next(&mut self, c: &Client) -> Result<Option<Partition<R>>> {
        let Some(rows) = self.next_rows(c).await? else {
            return Ok(None);
        };

        Ok(Some(Partition {
            data: rows.into_iter().map(|row| row.0).collect::<Vec<R>>(),
            index: self.next_index + 1, // add one since the first is included in first response
            total_count: self.partition_count,
        }))
    }

    async fn next_rows(&mut self, c: &Client) -> Result<Option<Vec<Row<R>>>> {
        if let Some(first_res) = self.first_res.take() {
            return Ok(Some(first_res));
        };

        self.next_index += 1;

        if self.info.meta.partition_info.len() <= self.next_index {
            return Ok(None);
        }

        let raw_rows = c
            .get_partition(&self.info.statement_handle, self.next_index)
            .send()
            .await?
            .snowflake_response::<RawPartition<Row<R>>>()
            .await?;

        Ok(Some(raw_rows.data))
    }
}
