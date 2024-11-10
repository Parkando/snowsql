use data_manipulation::DataManipulationResult;
use reqwest::header::{HeaderName, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use std::collections::HashMap;

pub use snowsql_derive::{FromRow, Selectable};
pub use snowsql_deserialize::{
    BindingKind, BindingValue, DeserializeFromStr, Error as DeserializeError, FromRow,
    Result as DeserializeResult,
};

pub mod data_manipulation;
mod error;
mod jwt;
mod selectable;

pub use {
    error::{CredentialsError, Error},
    selectable::*,
};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct SnowflakeConnector {
    host: String,
    pub token: String,
    client: reqwest::Client,
}

pub struct PrivateKey(pub String);
pub struct PublicKey(pub String);

impl SnowflakeConnector {
    pub fn try_new(
        private_key: PrivateKey,
        public_key: PublicKey,
        host: &str,
        account_identifier: &str,
        user: &str,
    ) -> Result<Self> {
        let token = jwt::create_token(
            public_key,
            private_key,
            &account_identifier.to_ascii_uppercase(),
            &user.to_ascii_uppercase(),
        )?;

        let auth_header = HeaderValue::from_str(&format!("Bearer {token}"))
            .expect("could not serialize token into header");

        let user_agent = concat!(env!("CARGO_PKG_NAME"), '/', env!("CARGO_PKG_VERSION"));

        let headers = [
            (CONTENT_TYPE, HeaderValue::from_static("application/json")),
            (AUTHORIZATION, auth_header),
            (ACCEPT, HeaderValue::from_static("application/json")),
            (USER_AGENT, HeaderValue::from_static(user_agent)),
            (
                HeaderName::from_static("x-snowflake-authorization-token-type"),
                HeaderValue::from_static("KEYPAIR_JWT"),
            ),
        ];

        let client = reqwest::Client::builder()
            .default_headers(headers.into_iter().collect())
            .gzip(true)
            .build()?;

        Ok(SnowflakeConnector {
            host: format!("https://{host}.snowflakecomputing.com/api/v2/"),
            token,
            client,
        })
    }

    pub fn sql(&self, statement: impl Into<String>) -> PendingQuery<'_> {
        PendingQuery {
            client: &self.client,
            host: &self.host,
            query: SnowflakeQuery {
                statement: statement.into(),
                timeout: None,
                role: None,
                bindings: Default::default(),
            },
            uuid: uuid::Uuid::new_v4(),
        }
    }
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Partition {
    data: Vec<RawRow>,
}

#[derive(Debug)]
pub struct PendingQuery<'c> {
    client: &'c reqwest::Client,
    host: &'c str,
    query: SnowflakeQuery,
    uuid: uuid::Uuid,
}

impl<'c> PendingQuery<'c> {
    pub async fn text(self) -> Result<String> {
        let res = self
            .client
            .post(self.get_url())
            .json(&self.query)
            .send()
            .await?
            .text()
            .await?;
        Ok(res)
    }

    pub async fn select(self) -> Result<Response<RawRow>> {
        let res = self
            .client
            .post(self.get_url())
            .json(&self.query)
            .send()
            .await?;

        let status = res.status();
        let bs = res.bytes().await?;

        if !status.is_success() {
            let body = String::from_utf8_lossy(&bs).into();
            return Err(Error::NokResponse { status, body });
        }

        let mut response = match serde_json::from_slice::<Response<RawRow>>(&bs) {
            Ok(deserialized) => deserialized,
            Err(err) => {
                return Err(Error::DeserializeSnowflakeResponse {
                    err,
                    body: String::from_utf8_lossy(&bs).into(),
                })
            }
        };

        if response.has_partitions() {
            self.fetch_and_merge_partitions(&mut response).await?;
        }

        Ok(response)
    }

    async fn fetch_and_merge_partitions(&self, result: &mut Response<RawRow>) -> Result<()> {
        for index in 1..result.info.result_set_meta_data.partition_info.len() {
            let url = self.get_partition_url(&result.info.statement_handle, index);

            let res = self.client.get(url).json(&self.query).send().await?;

            let status = res.status();
            let bs = res.bytes().await?;

            if !status.is_success() {
                return Err(Error::NokResponse {
                    status,
                    body: String::from_utf8_lossy(&bs).into(),
                });
            }

            let partition = match serde_json::from_slice::<Partition>(&bs) {
                Ok(deserialized) => deserialized,
                Err(err) => {
                    return Err(Error::DeserializeSnowflakeResponse {
                        err,
                        body: String::from_utf8_lossy(&bs).into(),
                    })
                }
            };

            result.data.extend(partition.data);
        }

        Ok(())
    }

    /// Use with `delete`, `insert`, `update` row(s).
    pub async fn manipulate(self) -> Result<DataManipulationResult> {
        let res = self
            .client
            .post(self.get_url())
            .json(&self.query)
            .send()
            .await?
            .json()
            .await?;
        Ok(res)
    }

    pub fn with_timeout(mut self, timeout: u32) -> Self {
        self.query.timeout = Some(timeout);
        self
    }

    pub fn with_role<R: ToString>(mut self, role: R) -> Self {
        self.query.role = Some(role.to_string());
        self
    }

    pub fn add_binding<T: Into<BindingValue>>(mut self, value: T) -> Self {
        let value: BindingValue = value.into();

        let binding = Binding {
            kind: value.kind(),
            value: value.to_string(),
        };

        self.query
            .bindings
            .insert((self.query.bindings.len() + 1).to_string(), binding);

        self
    }
    fn get_url(&self) -> String {
        // TODO: make another return type that allows retrying by calling same statement again with retry flag!
        format!("{}statements?requestId={}", self.host, self.uuid)
    }

    fn get_partition_url(&self, request_handle: &str, index: usize) -> String {
        format!(
            "{}statements/{request_handle}?partition={}",
            self.host, index
        )
    }
}

#[derive(serde::Serialize, Debug)]
pub struct SnowflakeQuery {
    statement: String,
    timeout: Option<u32>,
    role: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    bindings: HashMap<String, Binding>,
}

#[derive(serde::Serialize, Debug)]
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

impl<T> Response<T> {
    fn has_partitions(&self) -> bool {
        1 < self.info.result_set_meta_data.partition_info.len()
    }
}

impl Response<RawRow> {
    pub fn data<R: FromRow>(self) -> Result<Response<R>> {
        Ok(Response {
            data: self
                .data
                .into_iter()
                .map(|rr: RawRow| R::from_row(rr.0))
                .collect::<snowsql_deserialize::Result<Vec<R>>>()?,
            info: self.info,
        })
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
