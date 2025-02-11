use reqwest::header::{HeaderName, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use snowsql_deserialize::RawRow;

use crate::{jwt, PrivateKey, PublicKey, Result};

#[derive(Debug, Clone)]
pub struct Client {
    host: String,
    pub jwt: String,
    http: reqwest::Client,
}

impl Client {
    pub fn new_with_keys(
        private_key: PrivateKey,
        public_key: PublicKey,
        host: &str,
        account_identifier: &str,
        user: &str,
    ) -> Result<Self> {
        let token = jwt::create_token(public_key, private_key, account_identifier, user)?;

        Self::new_with_jwt(host, token)
    }

    pub fn new_with_jwt(host: &str, jwt: impl Into<String>) -> Result<Self> {
        let jwt = jwt.into();
        let auth_header = HeaderValue::from_str(&format!("Bearer {jwt}"))
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

        let http = reqwest::Client::builder()
            .default_headers(headers.into_iter().collect())
            .gzip(true)
            .build()?;

        Ok(Self {
            host: format!("https://{host}.snowflakecomputing.com/api/v2/"),
            jwt,
            http,
        })
    }

    pub(crate) fn get_partition(
        &self,
        request_handle: &str,
        index: usize,
    ) -> reqwest::RequestBuilder {
        self.http.get(format!(
            "{}statements/{}?partition={}",
            self.host, request_handle, index
        ))
    }

    pub(crate) fn post(&self) -> reqwest::RequestBuilder {
        self.http.post(format!(
            "{}statements?requestId={}",
            self.host,
            uuid::Uuid::new_v4()
        ))
    }

    pub async fn verify(&self) -> Result<bool> {
        let res = crate::sql::<RawRow>("SELECT 1").query(self).await?;
        Ok(!res.data.is_empty())
    }
}
