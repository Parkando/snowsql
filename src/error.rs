use std::borrow::Cow;

use reqwest::StatusCode;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid credentials: {0}")]
    Credentials(#[from] CredentialsError),

    #[error("http: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Nok response `{status}` with body:\n{body}")]
    NokResponse { status: StatusCode, body: String },

    #[error("parsing snowflake response: {err} with body:\n{body}")]
    DeserializeSnowflakeResponse {
        err: serde_json::Error,
        body: String,
    },

    #[error("deserialize: {0}")]
    Deserialize(#[from] snowsql_deserialize::Error),

    #[error("Internal Mutex error")]
    InternalMutexError,
}

#[derive(Debug, thiserror::Error)]
pub enum CredentialsError {
    #[error("invalid Public Key")]
    PublicKey,
    #[error("invalid Private Key")]
    PrivateKey,

    #[error("error when creating token: {0}")]
    Token(Cow<'static, str>),
}
