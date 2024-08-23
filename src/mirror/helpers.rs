use std::marker::PhantomData;
use uuid::Uuid;

use crate::command::Args;
use crate::mirror::HandleError;
use crate::net::{Port, SocketAddr, TimeStampMillis};

pub(crate) struct ArgIterator<'a> {
    args: &'a Args,
    index: usize,
}

#[derive(Debug)]
pub(crate) enum ArgumentError<T> {
    Missing(usize, PhantomData<T>),
    Malformed(usize, PhantomData<T>),
}

pub(crate) trait AsDeserializedIter {
    fn deser_iter(&self) -> ArgIterator;
}

impl AsDeserializedIter for Args {
    fn deser_iter(&self) -> ArgIterator {
        ArgIterator {
            args: self,
            index: 0,
        }
    }
}

pub(crate) trait DeserializedIter<'a> {
    fn next<T: ParamDeserialize<'a>>(&mut self) -> Result<T, ArgumentError<T>>;
}

impl<'a> DeserializedIter<'a> for ArgIterator<'a> {
    fn next<T: ParamDeserialize<'a>>(&mut self) -> Result<T, ArgumentError<T>> {
        let i = self.index;
        self.index += 1;
        if self.args.len() <= i {
            return Err(ArgumentError::Missing(i, PhantomData));
        }

        T::deser(&self.args[i][..]).ok_or(ArgumentError::Malformed(i, PhantomData))
    }
}

pub(crate) trait ParamDeserialize<'a> {
    fn deser(slice: &'a [u8]) -> Option<Self>
    where
        Self: Sized;
}

pub(crate) trait ParameterArray<'a> {
    fn deserialize<T: ParamDeserialize<'a>>(
        &'a self,
        i: usize,
        name: &'static str,
    ) -> Result<T, HandleError>;
}

impl<'a> ParameterArray<'a> for Args {
    fn deserialize<T: ParamDeserialize<'a>>(
        &'a self,
        i: usize,
        name: &'static str,
    ) -> Result<T, HandleError> {
        if i >= self.len() {
            return Err(HandleError::MissingParameter(i, name));
        }

        T::deser(&self[i][..]).ok_or_else(|| HandleError::MalformedParameter(i, name))
    }
}

impl ParamDeserialize<'_> for Uuid {
    fn deser(slice: &[u8]) -> Option<Self> {
        Uuid::from_slice(slice)
            .ok()
            .or_else(|| Uuid::parse_str(&String::from_utf8_lossy(slice)).ok())
    }
}

impl ParamDeserialize<'_> for Port {
    fn deser(slice: &[u8]) -> Option<Self> {
        match slice.len() {
            2 => Some(u16::from_be_bytes([slice[0], slice[1]])),
            _ => String::from_utf8_lossy(slice).parse().ok(),
        }
    }
}

impl ParamDeserialize<'_> for SocketAddr {
    fn deser(slice: &[u8]) -> Option<Self> {
        std::str::from_utf8(slice).ok()?.parse().ok()
    }
}

impl<'a> ParamDeserialize<'a> for &'a str {
    fn deser(slice: &'a [u8]) -> Option<&'a str> {
        std::str::from_utf8(slice).ok()
    }
}

impl<'a> ParamDeserialize<'a> for usize {
    fn deser(slice: &'a [u8]) -> Option<Self>
        where
            Self: Sized {
        std::str::from_utf8(slice).ok()?.parse().ok()
    }
}

impl ParamDeserialize<'_> for TimeStampMillis {
    fn deser(slice: &[u8]) -> Option<Self> {
        std::str::from_utf8(slice)
            .ok()?
            .parse::<TimeStampMillis>()
            .ok()
    }
}

impl<T> From<ArgumentError<T>> for HandleError {
    fn from(e: ArgumentError<T>) -> Self {
        match e {
            ArgumentError::Missing(i, PhantomData) => {
                HandleError::MissingParameter(i, std::any::type_name::<T>())
            }
            ArgumentError::Malformed(i, PhantomData) => {
                HandleError::MalformedParameter(i, std::any::type_name::<T>())
            }
        }
    }
}
