//! Really simple message protocol

mod helpers;
mod parser;

use bytes::Bytes;

pub(crate) use helpers::*;
pub(crate) use parser::ParseError;

impl<T: Into<Rsmp>> From<Vec<T>> for Rsmp {
    fn from(v: Vec<T>) -> Rsmp {
        Rsmp::Array(v.into_iter().map(Into::into).collect())
    }
}

impl From<String> for Rsmp {
    fn from(s: String) -> Rsmp {
        Rsmp::String(s.into_bytes().into())
    }
}

impl From<&str> for Rsmp {
    fn from(s: &str) -> Rsmp {
        Rsmp::String(Bytes::copy_from_slice(s.as_bytes()))
    }
}

#[derive(Clone, PartialEq)]
pub enum Rsmp {
    Null,
    String(Bytes),
    Error(Bytes),
    Int(i64),
    Float(f64),
    Array(Vec<Rsmp>),
    RawBytes(Bytes),
}

impl Default for Rsmp {
    fn default() -> Self {
        Rsmp::Error(b"Default"[..].into())
    }
}

pub(crate) trait IntoPrimitive {
    fn into_null(self) -> Option<()>;
    fn into_string(self) -> Option<String>;
    fn into_error(self) -> Option<String>;
    fn into_int(self) -> Option<i64>;
    fn into_float(self) -> Option<f64>;
    fn into_array(self) -> Option<Vec<Rsmp>>;
    fn into_raw_bytes(self) -> Option<Bytes>;
}

pub(crate) trait AsPrimitive {
    fn as_null(&self) -> Option<()>;
    fn as_string(&self) -> Option<&[u8]>;
    fn as_error(&self) -> Option<&[u8]>;
    fn as_int(&self) -> Option<i64>;
    fn as_float(&self) -> Option<f64>;
    fn as_array(&self) -> Option<&[Rsmp]>;
    fn as_raw_bytes(&self) -> Option<&[u8]>;
}

impl AsRef<Rsmp> for Rsmp {
    fn as_ref(&self) -> &Rsmp {
        self
    }
}

impl Rsmp {
    pub fn parse_bytes(input: &[u8]) -> Result<Rsmp, parser::ParseError>
where {
        parser::parse(input)
    }

    pub fn core_bytes(&self) -> Option<Bytes> {
        match self {
            Self::String(b) => Some(b.clone()),
            Self::Error(b) => Some(b.clone()),
            Self::RawBytes(b) => Some(b.clone()),
            _ => None,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Rsmp::Null => b"~\r\n".to_vec(),
            Rsmp::String(s) => {
                let mut v = Vec::new();
                v.extend_from_slice(b"$");
                v.extend_from_slice(s);
                v.extend_from_slice(b"\r\n");
                v
            }
            Rsmp::Error(s) => {
                let mut v = Vec::new();
                v.extend_from_slice(b"!");
                v.extend_from_slice(s);
                v.extend_from_slice(b"\r\n");
                v
            }
            Rsmp::Int(i) => {
                let mut v = Vec::new();
                v.extend_from_slice(b":");
                v.extend_from_slice(i.to_string().as_bytes());
                v.extend_from_slice(b"\r\n");
                v
            }
            Rsmp::Float(f) => {
                let mut v = Vec::new();
                v.extend_from_slice(b"^");
                v.extend_from_slice(f.to_string().as_bytes());
                v.extend_from_slice(b"\r\n");
                v
            }
            Rsmp::Array(a) => {
                let mut v = Vec::new();
                v.extend_from_slice(b"+");
                v.extend_from_slice(a.len().to_string().as_bytes());
                v.extend_from_slice(b"\r\n");
                for e in a {
                    v.extend_from_slice(&e.to_bytes());
                }
                v
            }
            Rsmp::RawBytes(b) => {
                let mut v = vec![64]; // @
                v.extend(b);
                v
            }
        }
    }

    pub fn to_debug_string(&self) -> String {
        match self {
            Rsmp::Null => "Null".to_string(),
            Rsmp::RawBytes(b) => format!("RawBytes({:?})", String::from_utf8_lossy(b)),
            Rsmp::String(s) => format!("String({:?})", String::from_utf8_lossy(s)),
            Rsmp::Error(s) => format!("Error({:?})", String::from_utf8_lossy(s)),
            Rsmp::Int(i) => format!("Integer({:?})", i),
            Rsmp::Float(f) => format!("Float({:?})", f),
            Rsmp::Array(a) => {
                let s = a
                    .iter()
                    .map(Rsmp::to_debug_string)
                    .collect::<Vec<String>>()
                    .join(", ");
                format!("[{}]", s)
            }
        }
    }
}

impl From<Rsmp> for Vec<u8> {
    fn from(r: Rsmp) -> Vec<u8> {
        r.to_bytes()
    }
}

impl std::fmt::Debug for Rsmp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.to_debug_string())
    }
}

impl AsPrimitive for Rsmp {
    fn as_null(&self) -> Option<()> {
        match self {
            Rsmp::Null => Some(()),
            _ => None,
        }
    }

    fn as_string(&self) -> Option<&[u8]> {
        match self {
            Rsmp::String(s) => Some(s),
            _ => None,
        }
    }

    fn as_error(&self) -> Option<&[u8]> {
        match self {
            Rsmp::Error(s) => Some(s),
            _ => None,
        }
    }

    fn as_int(&self) -> Option<i64> {
        match self {
            Rsmp::Int(i) => Some(*i),
            _ => None,
        }
    }

    fn as_float(&self) -> Option<f64> {
        match self {
            Rsmp::Float(i) => Some(*i),
            _ => None,
        }
    }

    fn as_array(&self) -> Option<&[Rsmp]> {
        match self {
            Rsmp::Array(a) => Some(a),
            _ => None,
        }
    }

    fn as_raw_bytes(&self) -> Option<&[u8]> {
        match self {
            Rsmp::RawBytes(b) => Some(b),
            _ => None,
        }
    }
}

impl IntoPrimitive for Rsmp {
    fn into_null(self) -> Option<()> {
        match self {
            Rsmp::Null => Some(()),
            _ => None,
        }
    }

    fn into_string(self) -> Option<String> {
        match self {
            Rsmp::String(s) => Some(String::from_utf8_lossy(&s).to_string()),
            _ => None,
        }
    }

    fn into_error(self) -> Option<String> {
        match self {
            Rsmp::Error(s) => Some(String::from_utf8_lossy(&s).to_string()),
            _ => None,
        }
    }

    fn into_int(self) -> Option<i64> {
        match self {
            Rsmp::Int(i) => Some(i),
            _ => None,
        }
    }

    fn into_float(self) -> Option<f64> {
        match self {
            Rsmp::Float(i) => Some(i),
            _ => None,
        }
    }

    fn into_array(self) -> Option<Vec<Rsmp>> {
        match self {
            Rsmp::Array(a) => Some(a),
            _ => None,
        }
    }

    fn into_raw_bytes(self) -> Option<Bytes> {
        match self {
            Rsmp::RawBytes(b) => Some(b),
            _ => None,
        }
    }
}
