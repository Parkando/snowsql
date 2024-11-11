use std::marker::PhantomData;

use snowsql_deserialize::FromRow;

use crate::{Client, QueryBuilder, Result};

#[derive(Clone, Copy, Debug)]
pub struct Pagination {
    pub page: usize,
    pub page_size: usize,
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            page: 1,
            page_size: 500,
        }
    }
}

impl Pagination {
    pub fn with_page_size(self, page_size: usize) -> Self {
        Self {
            page: self.page,
            page_size,
        }
    }

    pub fn with_page(self, page: usize) -> Self {
        Self {
            page,
            page_size: self.page_size,
        }
    }

    pub fn offset(&self) -> usize {
        (self.page - 1) * self.page_size
    }

    pub fn next_page(self) -> Option<Self> {
        if self.page == usize::MAX {
            None
        } else {
            Some(Self {
                page: self.page + 1,
                page_size: self.page_size,
            })
        }
    }

    pub fn previous_page(self) -> Option<Self> {
        if self.page == 1 {
            None
        } else {
            Some(Self {
                page: self.page - 1,
                page_size: self.page_size,
            })
        }
    }
}

pub struct Paginated<'a, R> {
    pub(crate) query: QueryBuilder,
    pub(crate) pagination: Pagination,
    pub(crate) client: &'a Client,

    is_done: bool,

    _marker: PhantomData<R>,
}

pub struct Page<R> {
    pub rows: Vec<R>,
    pub pagination: Pagination,
}

impl<'a, R> Paginated<'a, R>
where
    R: FromRow,
{
    pub fn new(client: &'a Client, query: QueryBuilder, pagination: Pagination) -> Self {
        Self {
            query,
            pagination,
            client,

            is_done: false,
            _marker: PhantomData,
        }
    }

    pub async fn next(&mut self) -> Result<Option<Page<R>>> {
        if self.is_done {
            return Ok(None);
        }

        let pagination = self.pagination;

        let res = self
            .query
            .clone()
            .offset(pagination.offset())
            .limit(pagination.page_size)
            .query(self.client)
            .await?;

        if res.data.len() < pagination.page_size {
            self.is_done = true;
        }

        if let Some(next_pagination) = pagination.next_page() {
            self.pagination = next_pagination;
        } else {
            self.is_done = true;
        }

        let rows = res.rows()?;

        Ok(Some(Page { rows, pagination }))
    }
}
