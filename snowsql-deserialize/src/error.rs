#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unexpected null value")]
    UnexpectedNull,

    #[error("invalid format `{given}`: {err}")]
    Format { given: String, err: String },

    #[error("invalid value `{given}`. {err}")]
    Value { given: String, err: String },

    #[error("deserializing field `{field}`: {err}")]
    Field { field: &'static str, err: Box<Self> },

    #[error("{0}")]
    Other(String),
}

#[derive(thiserror::Error, Debug)]
pub enum FromRowError {
    #[error("Field `{field}` could not be parsed as &str: {err}")]
    NonStrField { field: &'static str, err: String },

    #[error("invalid row length, expected column {idx} for field `{field}`")]
    MissingField { idx: usize, field: &'static str },

    #[error("deserializing field `{field}`: {err}")]
    DeserializingField { field: &'static str, err: Error },

    #[error("{0}")]
    Custom(String),
}

impl Error {
    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
}
