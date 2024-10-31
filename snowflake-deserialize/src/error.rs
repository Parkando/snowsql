#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unexpected null")]
    UnexpectedNull,

    #[error("invalid value `{given}`: {err}")]
    Format { given: String, err: String },

    #[error("deserializing field `{field}`: {err}")]
    Field { field: &'static str, err: Box<Self> },

    #[error("{0}")]
    Other(String),
}

impl Error {
    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
}
