use crate::rsmp::Rsmp;
use crate::store::{Bytes, Error, Input, Output, Sendable};

use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use serde_json;

impl<T: Serialize + Send + Sync + 'static> Output for T {
    fn into_bytes(self) -> Vec<u8> {
        match serde_json::to_string(&self) {
            Ok(obj) => obj.as_bytes().to_owned(),
            Err(e) => Rsmp::Error(format!("SerializeError: {e}").into()).to_bytes(),
        }
    }
}

impl<T: DeserializeOwned + Send + Sync + 'static> Input for T {
    fn from_bytes(slice: &Bytes) -> Result<T, Error> {
        match serde_json::from_slice(&slice[..]) {
            Ok(i) => Ok(i),
            Err(e) => Err(Error::new(format!("DeserializeError: {e}"))),
        }
    }
}

impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> Sendable for T {
        fn initialize(&mut self, slice: Vec<u8>) -> Result<(), Error> {
            let _ = std::mem::replace(self, serde_json::from_slice(&slice)?);
            Ok(())
        }

        fn serialize(&self) -> Result<Vec<u8>, Error> {
            Ok(serde_json::to_vec(&self)?)
        }
}