#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unexpected null")]
    UnexpectedNull,

    #[error("invalid date `{given}`: {err}")]
    Format { given: String, err: String },

    #[error("deserializing field `{field}`: {err}")]
    Field { field: &'static str, err: Box<Self> },
}
