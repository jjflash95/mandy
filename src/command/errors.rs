use crate::rsmp::Rsmp;

use thiserror::Error;

#[derive(Clone, Error, Debug)]
pub enum CommandError {
    #[error("Invalid command: {0}")]
    InvalidCommand(String),
    #[error("Invalid command length: {0}")]
    InvalidLength(String),
}

impl From<&CommandError> for Rsmp {
    fn from(err: &CommandError) -> Rsmp {
        Rsmp::Error(err.to_string().into_bytes().into())
    }
}
