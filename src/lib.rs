use data_manipulation::DataManipulationResult;
use std::{collections::HashMap, marker::PhantomData};

mod client;
pub mod data_manipulation;
mod error;
mod jwt;
mod partitions;
mod selectable;

pub use {
    client::Client,
    error::{CredentialsError, Error},
    partitions::Partitions,
    selectable::*,
    serde,
    snowsql_derive::{FromRow, Selectable},
    snowsql_deserialize::{
        BindingKind, BindingValue, Error as DeserializeError, FromRow, FromRowResult, FromValue,
        RawRow, Result as DeserializeResult, Row, RowAccess,
    },
};

pub type Result<T> = std::result::Result<T, Error>;

pub struct PrivateKey(pub String);
pub struct PublicKey(pub String);

pub fn sql<R>(statement: impl Into<String>) -> QueryBuilder<R>
where
    R: FromRow,
{
    QueryBuilder::new(statement)
}

trait ResponseOk {
    async fn snowflake_response<T>(self) -> Result<T>
    where
        T: serde::de::DeserializeOwned;
}

impl ResponseOk for reqwest::Response {
    async fn snowflake_response<T>(self) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let status = self.status();
        let bs = self.bytes().await?;

        {
            let s = String::from_utf8_lossy(&bs);

            println!("raw data: `{s}`");
        }

        if !status.is_success() {
            let body = String::from_utf8_lossy(&bs).into();
            return Err(Error::NokResponse { status, body });
        }

        match serde_json::from_slice::<T>(&bs) {
            Ok(deserialized) => Ok(deserialized),
            Err(err) => {
                let lines_to_skip = err.line().max(1) - 1;
                let chars_to_skip = err.column().max(100) - 100;

                let mut iter = bs.iter();

                let mut line_counter = 0;
                let mut b_counter = 0;

                let extract_bs = (&mut iter)
                    .skip_while(|&&b| {
                        if b == b'\n' {
                            line_counter += 1;
                        }

                        line_counter < lines_to_skip
                    })
                    .skip_while(|_| {
                        b_counter += 1;
                        b_counter < chars_to_skip
                    })
                    .copied()
                    .take(200)
                    .collect::<Vec<u8>>();

                Err(Error::DeserializeSnowflakeResponse {
                    err,
                    body: String::from_utf8_lossy(&extract_bs).into(),
                })
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct QueryBuilder<R> {
    pub statement: String,
    timeout: Option<u32>,
    role: Option<String>,
    bindings: HashMap<String, Binding>,
    order_by: Option<String>,
    offset: Option<usize>,
    limit: Option<usize>,

    parameters: Option<StatementParameters>,

    _marker: PhantomData<R>,
}

impl<R> QueryBuilder<R>
where
    R: FromRow,
{
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            statement: query.into(),
            timeout: None,
            role: None,
            bindings: HashMap::default(),
            order_by: None,
            offset: None,
            limit: None,
            parameters: None,
            _marker: PhantomData,
        }
    }

    fn build_query(self) -> SnowflakeQuery {
        let mut statement = self.statement.clone();

        if let Some(order_by) = self.order_by.as_deref() {
            statement.push_str(" ORDER BY '");
            statement.push_str(order_by);
            statement.push('\'');
        }

        if let Some(limit) = self.limit {
            statement.push_str(&format!(" LIMIT {limit}"));
        }

        if let Some(offset) = self.offset {
            statement.push_str(&format!(" OFFSET {offset}"));
        }

        SnowflakeQuery {
            statement,
            timeout: self.timeout,
            role: self.role,
            bindings: self.bindings,
            parameters: self.parameters,
        }
    }

    pub async fn text(self, c: &Client) -> Result<String> {
        Ok(c.post()
            .json(&self.build_query())
            .send()
            .await?
            .text()
            .await?)
    }

    pub async fn query(self, c: &Client) -> Result<Response<Row<R>>> {
        let qry = self.build_query();

        let response = c
            .post()
            .json(&qry)
            .send()
            .await?
            .snowflake_response::<Response<Row<R>>>()
            .await?;

        Ok(response)
    }

    /// Use with `delete`, `insert`, `update` row(s).
    pub async fn manipulate(self, c: &Client) -> Result<DataManipulationResult> {
        let res = c
            .post()
            .json(&self.build_query())
            .send()
            .await?
            .snowflake_response::<DataManipulationResult>()
            .await?;
        Ok(res)
    }

    pub fn sql(mut self, s: impl AsRef<str>) -> Self {
        self.statement.push(' ');
        self.statement.push_str(s.as_ref());
        self
    }

    pub fn order_by(mut self, field: impl Into<String>) -> Self {
        self.order_by = Some(field.into());
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_timeout(mut self, timeout: u32) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }

    /// (Optional) Specifies the maximum size of each set (or chunk) of query results to download (in MB).
    /// For details, see
    /// [CLIENT_RESULT_CHUNK_SIZE](https://docs.snowflake.com/sql-reference/parameters.html#label-client-result-chunk-size).
    ///
    /// Type: integer
    ///
    /// Example: 100
    pub fn with_result_chunk_size(mut self, chunk_size: usize) -> Self {
        self.parameters
            .get_or_insert_with(StatementParameters::default)
            .chunk_size = Some(chunk_size);
        self
    }

    /// (Optional) Specifies the maximum number of rows returned in a result set,
    /// with 0 (default) meaning no maximum. For details, see
    /// [ROWS_PER_RESULTSET parameter](https://docs.snowflake.com/sql-reference/parameters.html#label-rows-per-resultset).
    ///
    /// Type: integer
    ///
    /// Example: 200
    pub fn with_result_row_count(mut self, rows_per_result_set: usize) -> Self {
        self.parameters
            .get_or_insert_with(StatementParameters::default)
            .rows_per_set = Some(rows_per_result_set);
        self
    }

    pub fn add_binding<T: Into<BindingValue>>(mut self, value: T) -> Self {
        let value: BindingValue = value.into();

        let binding = Binding {
            kind: value.kind(),
            value: value.to_string(),
        };

        self.bindings
            .insert((self.bindings.len() + 1).to_string(), binding);

        self
    }
}

#[derive(Default, Clone, serde::Serialize, Debug)]
pub struct StatementParameters {
    #[serde(rename = "client_result_chunk_size")]
    pub chunk_size: Option<usize>,

    #[serde(rename = "rows_per_resultset")]
    pub rows_per_set: Option<usize>,
}

#[derive(Clone, serde::Serialize, Debug)]
pub struct SnowflakeQuery {
    statement: String,
    timeout: Option<u32>,
    role: Option<String>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    bindings: HashMap<String, Binding>,

    #[serde(skip_serializing_if = "Option::is_none")]
    parameters: Option<StatementParameters>,
}

#[derive(Clone, serde::Serialize, Debug)]
pub struct Binding {
    #[serde(rename = "type")]
    kind: BindingKind,
    value: String,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PartitionInfo {
    pub row_count: usize,
    pub uncompressed_size: usize,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Response<R> {
    pub data: Vec<R>,

    #[serde(flatten)]
    pub info: ResponseInfo,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ResponseInfo {
    #[serde(rename = "resultSetMetaData")]
    pub meta: MetaData,
    pub code: String,
    pub statement_status_url: String,
    pub request_id: String,
    pub sql_state: String,
    pub message: String,
    pub statement_handle: String,
    //pub created_on: u64,
}

impl<R> Response<Row<R>>
where
    R: FromRow,
{
    pub fn partitions(self) -> Partitions<R> {
        Partitions::from_response(self)
    }
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MetaData {
    pub num_rows: usize,
    pub format: String,
    pub row_type: Vec<RowType>,
    partition_info: Vec<PartitionInfo>,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RowType {
    pub name: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub precision: Option<u32>,
    pub byte_length: Option<usize>,
    #[serde(rename = "type")]
    pub data_type: String,
    pub scale: Option<i32>,
    pub nullable: bool,
    //pub collation: ???,
    //pub length: ???,
}
