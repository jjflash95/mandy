use crate::rsmp::Rsmp;
use crate::store::{Bytes, Error, Input, Output};
use std::fmt::Debug;

use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use serde_json;

impl<T: Serialize + Debug + Send + Sync + 'static> Output for T {
    fn into_bytes(self) -> Vec<u8> {
        match serde_json::to_string(&self) {
            Ok(obj) => obj.as_bytes().to_owned(),
            Err(e) => Rsmp::Error(format!("SerializeError: {e}").into()).to_bytes(),
        }
    }
}

impl<T: DeserializeOwned + Debug + Send + Sync + 'static> Input for T {
    fn from_bytes(slice: &Bytes) -> Result<T, Error> {
        match serde_json::from_slice(&slice[..]) {
            Ok(i) => Ok(i),
            Err(e) => Err(Error::new(format!("DeserializeError: {e}"))),
        }
    }
}
