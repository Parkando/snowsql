use data_manipulation::DataManipulationResult;
use std::collections::HashMap;

pub use snowsql_derive::{FromRow, Selectable};
pub use snowsql_deserialize::{
    BindingKind, BindingValue, DeserializeFromStr, Error as DeserializeError, FromRow,
    Result as DeserializeResult,
};

mod client;
pub mod data_manipulation;
mod error;
mod jwt;
mod pagination;
mod selectable;

pub use {
    client::Client,
    error::{CredentialsError, Error},
    pagination::*,
    selectable::*,
};

pub type Result<T> = std::result::Result<T, Error>;

pub struct PrivateKey(pub String);
pub struct PublicKey(pub String);

pub fn sql(statement: impl Into<String>) -> QueryBuilder {
    QueryBuilder::new(statement)
}

trait ResponseOk {
    async fn ok<T: serde::de::DeserializeOwned>(self) -> Result<T>;
}

impl ResponseOk for reqwest::Response {
    async fn ok<T: serde::de::DeserializeOwned>(self) -> Result<T> {
        let status = self.status();
        let bs = self.bytes().await?;

        if !status.is_success() {
            let body = String::from_utf8_lossy(&bs).into();
            return Err(Error::NokResponse { status, body });
        }

        match serde_json::from_slice::<T>(&bs) {
            Ok(deserialized) => Ok(deserialized),
            Err(err) => Err(Error::DeserializeSnowflakeResponse {
                err,
                body: String::from_utf8_lossy(&bs).into(),
            }),
        }
    }
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Partition {
    data: Vec<RawRow>,
}

#[derive(Clone, Debug)]
pub struct QueryBuilder {
    statement: String,
    timeout: Option<u32>,
    role: Option<String>,
    bindings: HashMap<String, Binding>,
    order_by: Option<String>,
    offset: Option<usize>,
    limit: Option<usize>,
}

impl QueryBuilder {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            statement: query.into(),
            timeout: None,
            role: None,
            bindings: HashMap::default(),
            order_by: None,
            offset: None,
            limit: None,
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

    pub async fn query(self, c: &Client) -> Result<Response<RawRow>> {
        let qry = self.build_query();

        let mut response = c
            .post()
            .json(&qry)
            .send()
            .await?
            .ok::<Response<RawRow>>()
            .await?;

        for index in 1..response.info.result_set_meta_data.partition_info.len() {
            let partition = c
                .get_partition(&response.info.statement_handle, index)
                .send()
                .await?
                .ok::<Partition>()
                .await?;

            response.data.extend(partition.data);
        }

        Ok(response)
    }

    pub fn paginated<R: FromRow>(self, client: &Client, pagination: Pagination) -> Paginated<R> {
        Paginated::new(client, self, pagination)
    }

    /// Use with `delete`, `insert`, `update` row(s).
    pub async fn manipulate(self, c: &Client) -> Result<DataManipulationResult> {
        let res = c
            .post()
            .json(&self.build_query())
            .send()
            .await?
            .ok()
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

    pub fn with_role<R: ToString>(mut self, role: R) -> Self {
        self.role = Some(role.to_string());
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

#[derive(Clone, serde::Serialize, Debug)]
pub struct SnowflakeQuery {
    statement: String,
    timeout: Option<u32>,
    role: Option<String>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    bindings: HashMap<String, Binding>,
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

#[derive(Debug, serde::Deserialize)]
pub struct RawRow(pub Vec<Option<String>>);

impl FromRow for RawRow {
    fn from_row(row: Vec<Option<String>>) -> DeserializeResult<Self>
    where
        Self: Sized,
    {
        Ok(Self(row))
    }
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Response<T> {
    pub data: Vec<T>,

    #[serde(flatten)]
    pub info: ResponseInfo,
}
#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ResponseInfo {
    pub result_set_meta_data: MetaData,
    pub code: String,
    pub statement_status_url: String,
    pub request_id: String,
    pub sql_state: String,
    pub message: String,
    pub statement_handle: String,
    //pub created_on: u64,
}

impl Response<RawRow> {
    pub fn split<R: FromRow>(self) -> Result<(ResponseInfo, Vec<R>)> {
        let info = self.info;

        let data = self
            .data
            .into_iter()
            .map(|rr: RawRow| R::from_row(rr.0))
            .collect::<snowsql_deserialize::Result<Vec<R>>>()?;

        Ok((info, data))
    }

    pub fn rows<R: FromRow>(self) -> Result<Vec<R>> {
        let rows = self
            .data
            .into_iter()
            .map(|rr: RawRow| R::from_row(rr.0))
            .collect::<snowsql_deserialize::Result<Vec<R>>>()?;

        Ok(rows)
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
