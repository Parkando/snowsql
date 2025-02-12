use std::{
    sync::{Arc, Mutex},
    time,
};

use reqwest::{
    header::{HeaderName, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE, USER_AGENT},
    Method,
};
use snowsql_deserialize::RawRow;

use crate::{jwt, Error, PrivateKey, PublicKey, Result};

#[derive(Clone)]
pub struct Client(Arc<Mutex<ClientInner>>);

pub struct ClientInner {
    credentials: Credentials,
    jwt: Jwt,
    host: String,
    http: reqwest::Client,
}

struct Jwt {
    token: String,
    expires_at: time::Instant,
}

impl Jwt {
    fn is_expired(&self) -> bool {
        self.expires_at < time::Instant::now()
    }
}

struct Credentials {
    private_key: PrivateKey,
    public_key: PublicKey,
    account_identifier: String,
    user: String,
}

impl Credentials {
    fn create_jwt(&self) -> Result<Jwt> {
        let token = jwt::create_token(
            &self.public_key,
            &self.private_key,
            &self.account_identifier,
            &self.user,
        )?;

        Ok(Jwt {
            token,
            expires_at: time::Instant::now() + time::Duration::from_secs(60 * 59),
        })
    }

    fn build_http_client(&self) -> Result<reqwest::Client> {
        let user_agent = concat!(env!("CARGO_PKG_NAME"), '/', env!("CARGO_PKG_VERSION"));

        let headers = [
            (CONTENT_TYPE, HeaderValue::from_static("application/json")),
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

        Ok(http)
    }
}

impl Client {
    pub fn new_with_keys(
        private_key: PrivateKey,
        public_key: PublicKey,
        host: impl AsRef<str>,
        account_identifier: impl Into<String>,
        user: impl Into<String>,
    ) -> Result<Self> {
        let credentials = Credentials {
            private_key,
            public_key,
            account_identifier: account_identifier.into(),
            user: user.into(),
        };

        let jwt = credentials.create_jwt()?;
        let http = credentials.build_http_client()?;

        let inner = ClientInner {
            credentials,
            host: format!("https://{}.snowflakecomputing.com/api/v2/", host.as_ref()),
            jwt,
            http,
        };

        Ok(Self(Arc::new(Mutex::new(inner))))
    }

    pub(crate) fn new_request(
        &self,
        method: reqwest::Method,
        path: &str,
    ) -> Result<reqwest::RequestBuilder> {
        let mut inner = self.0.lock().map_err(|_| Error::InternalMutexError)?;

        // If JWT expired. Create a new inner client.
        if inner.jwt.is_expired() {
            inner.jwt = inner.credentials.create_jwt()?;
        }

        Ok(inner
            .http
            .request(method, format!("{}{}", inner.host, path))
            .header(AUTHORIZATION, format!("Bearer {}", inner.jwt.token)))
    }

    pub(crate) fn get_partition(
        &self,
        request_handle: &str,
        index: usize,
    ) -> Result<reqwest::RequestBuilder> {
        self.new_request(
            Method::GET,
            &format!("/statements/{}?partition={}", request_handle, index),
        )
    }

    pub(crate) fn post(&self) -> Result<reqwest::RequestBuilder> {
        self.new_request(
            Method::POST,
            &format!("/statements?requestId={}", uuid::Uuid::new_v4()),
        )
    }

    pub async fn verify(&self) -> Result<bool> {
        let res = crate::sql::<RawRow>("SELECT 1").query(self).await?;
        Ok(!res.data.is_empty())
    }
}
