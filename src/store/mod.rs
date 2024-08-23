//! Traits for data persistence and serialize/deserialize ops

pub use bytes::Bytes;
use std::fmt::{Debug, Display};
use std::sync::Arc;
use tokio::sync::RwLock;
#[cfg(feature = "json")]
pub mod json;

pub type SharedStore<Store> = Arc<RwLock<Store>>;


/// General client facing error for Write, Read, Serialize and Initialize operations
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error(String);

/// Trait bound for data sharing and persistence
pub trait Storage: Sendable + Writable + Readable {}

/// Defines how to parse the `Output` type into a single `byte buffer` in an infallible way,
/// this is the result clients will see from their read/write operations.
///
/// The blanket implementation for types `T` implementing the `std::fmt::Display` trait is basically
/// converting the type to a `String` and returning it as bytes
/// ```ignore
/// T::to_string(&self).as_bytes().to_owned()
/// ```
///
/// # Example
///
/// ```
/// use mandy::Bytes;
/// use mandy::store::Output;
///
///
/// #[derive(Debug, PartialEq)]
/// pub enum Fruit {
///     Apple,
///     Banana,
///     Peach,
/// }
///
/// impl Output for Fruit {
///     fn into_bytes(self) -> Vec<u8> {
///         let bytes = match self {
///             Fruit::Apple => Bytes::from("🍎"),
///             Fruit::Banana => Bytes::from("🍌"),
///             Fruit::Peach => Bytes::from("🍑"),
///         };
///
///         bytes.to_vec()
///     }
/// }
///
/// assert_eq!(Fruit::into_bytes(Fruit::Peach), "🍑".into_bytes());
/// ```
pub trait Output: Send + Sync + 'static {
    fn into_bytes(self) -> Vec<u8>;
}

/// Defines how to parse a single `byte slice` into an `Input`,
/// this is what we will receive from clients for read/write operations.
///
/// The blanket implementation for types implementing the `std::str::FromStr` trait is basically
/// parsing an utf-8 string from the slice and calling `FromStr` on the result
/// ```ignore
/// FromStr::from_str(str::from_utf8(&bytes[..])?)
/// ```
///
/// # Example
///
/// ```
/// use mandy::Bytes;
/// use mandy::store::{Input, Error};
///
///
/// #[derive(Debug, PartialEq)]
/// pub enum Fruit {
///     Apple,
///     Banana,
///     Peach,
/// }
///
/// impl Input for Fruit {
///     fn from_bytes(bytes: &Bytes) -> Result<Self, Error> {
///         let parsed = String::from_utf8_lossy(bytes);
///         match parsed.chars().next() {
///             Some('🍎') => Ok(Fruit::Apple),
///             Some('🍌') => Ok(Fruit::Banana),
///             Some('🍑') => Ok(Fruit::Peach),
///             _ => Err(Error::new("Invalid fruit")),
///         }
///     }
/// }
///
/// assert_eq!(Fruit::from_bytes(&Bytes::from("🍎")), Ok(Fruit::Apple));
/// ```
///
/// # Blanket Impl for String example
/// ```
///
/// use mandy::Bytes;
/// use mandy::store::{Input, Error};
///
/// let valid_utf8 = Bytes::from_static(b"hello");
/// assert_eq!(
///     String::from_bytes(&valid_utf8),
///     Ok(String::from("hello"))
/// );
///
/// let invalid_utf8 = Bytes::from_static(b"hello\xff");
/// let expected = Error::new("invalid utf-8 sequence of 1 bytes from index 5");
///
/// assert_eq!(String::from_bytes(&invalid_utf8), Err(expected));
/// ```
pub trait Input: Send + Sync + 'static {
    fn from_bytes(bytes: &Bytes) -> Result<Self, Error>
    where
        Self: Sized;
}

/// Defines how to perform a read operation in a `Storage` type
///
/// # Associated types
/// - `Input` represents a single parsed input
/// - `Output` represent the result of the read operation for one or more `Input`s,
/// it must implement the `Output` trait so it can be serialized and sent back to the client
///
/// # Example
///
/// ```
/// use std::fmt::Display;
/// use mandy::store::Readable;
/// use mandy::store::Error;
/// use mandy::Bytes;
///
/// #[derive(Debug)]
/// pub struct Diary {
///     pub records: Vec<String>,
/// }
///
/// #[derive(Debug, Eq, PartialEq)]
/// pub struct Records(pub Vec<String>);
///
/// impl Display for Records {
///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
///         let s: String = self.0.iter().map(|s| format!("{s}\r\n")).collect();
///         write!(f, "{}", s)
///     }
/// }
///
/// impl Readable for Diary {
///     type Input = String;
///     type Output = Records;
///
///     // Returns all records that contain any of the query strings
///     fn read(&self, query: Vec<Self::Input>) -> Result<Self::Output, Error> {
///         let mut result = vec![];
///         for record in &self.records {
///             if query.iter().all(|q| record.contains(q)) {
///                 result.push(record.clone());
///             }
///         }
///         Ok(Records(result))
///     }
/// }
///
/// let mut diary = Diary { records: vec![] };
/// assert_eq!(
///     Diary::parse_input(&[Bytes::from("hello")]),
///     Ok(vec!["hello".to_string()])
/// );
///
/// let words = vec!["cat".to_string(), "car".to_string(), "bird".to_string()];
/// diary.records.extend_from_slice(&words);
///
/// let query = vec!["ca".to_string()];
/// assert_eq!(
///     diary.read(query).unwrap(),
///     Records(vec!["cat".to_string(), "car".to_string()])
/// );
///
/// ```
pub trait Readable: Send + Sync + 'static {
    type Input: Input;
    type Output: Output;

    /// Defines how to parse the raw byte slices received from the client into an input type.
    ///
    /// For example upon receiving a message containing:
    /// <br>
    /// ```b"+3\r\n$READ\r\n$420\r\n$69\r\n"```
    /// It will be roughly translated to:
    /// `["READ", "420", "69"]`, where the first element is the command and the rest are the arguments, so the received query
    /// will be equivalent to
    /// ```
    /// use bytes::Bytes;
    ///
    /// let query = &[Bytes::from_static(b"420"), Bytes::from_static(b"69")];
    /// ```
    ///
    ///
    /// # Errors
    ///
    /// The default implementation will just iterate over the bytes and call `Input::from_bytes` on each slice,
    /// if any slice parsing fails, the entire operation will fail returning the first error
    fn parse_input(query: &[Bytes]) -> Result<Vec<Self::Input>, Error> {
        query.iter().map(Self::Input::from_bytes).collect()
    }

    /// Defines how to fetch an `Output` from the storage with the already parsed input data
    ///
    /// # Concurrency
    ///
    /// To ensure thread safety a `RwLock::read` is used before calling this function.
    ///
    /// To avoid blocking other threads the implementation should be as straightforward and fast as possible.
    /// If any preprocessing is needed it should be done by overwriting the non-blocking `parse_input method`.
    fn read(&self, query: Vec<Self::Input>) -> Result<Self::Output, Error>;
}

/// Defines how to perform a write operation in a `Storage` type
///
/// # Associated types
/// - `Input` represents a single parsed input
/// - `Output` represent the result of the write operation for one or more `Input`s,
/// it must implement the `Output` trait so it can be serialized and sent back to the client
///
/// # Example
///
/// ```
/// use std::fmt::Display;
/// use mandy::store::Writable;
/// use mandy::store::Error;
/// use mandy::Bytes;
///
/// #[derive(Debug)]
/// pub struct Diary {
///     pub records: Vec<String>,
/// }
///
/// #[derive(Debug, Eq, PartialEq)]
/// pub struct Records(pub Vec<String>);
///
/// impl Display for Records {
///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
///         let s: String = self.0.iter().map(|s| format!("{s}\r\n")).collect();
///         write!(f, "{}", s)
///     }
/// }
///
/// impl Writable for Diary {
///     type Input = String;
///     type Output = usize;
///
///     // Writes all records to the diary returning the amount of records added
///     fn write(&mut self, query: Vec<Self::Input>) -> Result<Self::Output, Error> {
///         let amount = query.into_iter().fold(0, |acc, record| {
///             self.records.push(record);
///             acc + 1
///         });
///
///         Ok(amount)
///     }
/// }
///
/// let mut diary = Diary { records: vec![] };
/// let query = Diary::parse_input(&[
///     Bytes::from("this is the end"),
///     Bytes::from("beautiful friend"),
/// ])
/// .unwrap();
///
/// assert_eq!(
///     query,
///     vec![
///         "this is the end".to_string(),
///         "beautiful friend".to_string()
///     ]
/// );
///     
/// assert_eq!(diary.write(query), Ok(2));
///
/// ```
pub trait Writable: Send + Sync + 'static {
    type Input: Input;
    type Output: Output;

    /// Defines how to parse the raw byte slices received from the client into an input type.
    ///
    /// For example upon receiving a message containing:
    /// <br>
    /// ```b"+3\r\n$WRITE\r\n$420\r\n$69\r\n"```
    /// It will be roughly translated to:
    /// `["WRITE", "420", "69"]`, where the first element is the command and the rest are the arguments, so the received query
    /// will be equivalent to
    ///
    /// ```
    /// use bytes::Bytes;
    ///
    /// let query = &[Bytes::from_static(b"420"), Bytes::from_static(b"69")];
    /// ```
    ///
    /// # Errors
    ///
    /// The default implementation will just iterate over the bytes and call `Input::from_bytes` on each slice,
    /// if any slice parsing fails, the entire operation will fail returning the first error
    fn parse_input(query: &[Bytes]) -> Result<Vec<Self::Input>, Error> {
        query.iter().map(Self::Input::from_bytes).collect()
    }

    /// Defines how to persist the already parsed input data into the storage
    ///
    /// # Concurrency
    ///
    /// To ensure thread safety a `RwLock::write` is used before calling this function.
    ///
    /// To avoid blocking other threads the implementation should be as straightforward and fast as possible.
    /// If any preprocessing is needed it should be done by overwriting the non-blocking `parse_input method`.
    fn write(&mut self, query: Vec<Self::Input>) -> Result<Self::Output, Error>;
}

/// A data structure that can be both `initialized` and `serialized` from and into an array of bytes
/// used for sharing state between different nodes.
///
/// More specifically a subscribing `Replica`
/// will ask their parent node (can be `Master` or another `Replica`) for their current state,
/// which the parent will `serialize` and send via tcp, when the operation is complete the replica
/// will `initialize` itself with the received buffer
///
/// The serialization can be different for the `Output::into_bytes` implementation, since it is going to contain all
/// the information from the store and it will be sent via tcp sockets we can optimize for size however we want
///
///
/// # Example
/// ```
/// use mandy::store::{Sendable, Error};
///
/// #[derive(Debug, PartialEq)]
/// pub enum Fruit {
///     Apple,
///     Banana,
///     Peach,
/// }
///
/// impl TryFrom<u8> for Fruit {
///     type Error = Error;
///
///     fn try_from(value: u8) -> Result<Self, Self::Error> {
///         match value {
///             0 => Ok(Fruit::Apple),
///             1 => Ok(Fruit::Banana),
///             2 => Ok(Fruit::Peach),
///             _ => Err(Error::from("Invalid fruit")),
///         }
///     }
/// }
///
/// impl From<&Fruit> for u8 {
///     fn from(fruit: &Fruit) -> u8 {
///         match fruit {
///             Fruit::Apple => 0,
///             Fruit::Banana => 1,
///             Fruit::Peach => 2,
///         }
///     }
/// }
///
/// #[derive(Debug, Default, PartialEq)]
/// pub struct Basket {
///     pub contents: Vec<Fruit>,
/// }
///
/// impl Sendable for Basket {
///     fn initialize(&mut self, slice: Vec<u8>) -> Result<(), Error> {
///         let fruits = slice
///             .into_iter()
///             .map(Fruit::try_from)
///             .collect::<Result<Vec<_>, Error>>()?;
///
///         self.contents.extend(fruits);
///
///         Ok(())
///     }
///
///     fn serialize(&self) -> Result<Vec<u8>, Error> {
///         Ok(self.contents.iter().map(u8::from).collect())
///     }
/// }
///
/// // Serializing a basket into u8 array
/// let mut basket = Basket::default();
/// let fruits = vec![Fruit::Peach, Fruit::Apple, Fruit::Apple];
/// basket.contents.extend(fruits);
/// let serialized = basket.serialize().unwrap();
///
/// assert_eq!(&serialized, &[2, 0, 0]);
///
/// // Initializing a basket from the serialized array
/// let mut new_basket = Basket::default();
/// new_basket.initialize(serialized).unwrap();
///
/// assert_eq!(basket, new_basket);
/// ```
pub trait Sendable: Sized + Send + Sync + 'static {
    /// Defines how to initialize our storage with the full serialized state of a parent node, syncing to it.
    ///
    /// # Concurrency
    ///
    /// Even though this method is behind a `RwLock::write`, at the time this is called there is a guarantee
    /// that no other thread is `writing` or `reading` from the store, since this happens in a `uninitialized mirror`
    fn initialize(&mut self, slice: Vec<u8>) -> Result<(), Error>;
    /// Defines how to serialize the entire state into a single byte buffer that will be sent over tcp.
    ///
    /// This this is not client facing so the implementation can be optimized for size, this method is called
    ///
    /// # Concurrency
    ///
    /// This method is called behind a `RwLock::read` effectively locking other `Writers` until the serialization is complete
    fn serialize(&self) -> Result<Vec<u8>, Error>;
}

#[cfg(not(feature = "json"))]
impl<T> Input for T
where
    T: std::str::FromStr + Send + Sync + 'static,
{
    fn from_bytes(bytes: &Bytes) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let parsed = std::str::from_utf8(&bytes[..])?;

        let t = std::str::FromStr::from_str(parsed).map_err(|_| {
            format!(
                "Failed to convert from string to {}",
                std::any::type_name::<T>()
            )
        })?;
        Ok(t)
    }
}

#[cfg(not(feature = "json"))]
impl<T> Output for T
where
    T: Display + Send + Sync + 'static,
{
    fn into_bytes(self) -> Vec<u8> {
        T::to_string(&self).into_bytes()
    }
}

impl Error {
    pub fn new<T: Display>(e: T) -> Self {
        Self(e.to_string())
    }

    pub fn inner(&self) -> &str {
        &self.0
    }
}

impl<T: Display> From<T> for Error {
    fn from(c: T) -> Error {
        Self::new(c.to_string())
    }
}

impl std::ops::Deref for Error {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> Storage for T where T: Sendable + Writable + Readable {}

pub(crate) mod utils {
    use sha2::{Digest, Sha256};

    #[derive(Debug, Eq, PartialEq)]
    pub(crate) struct Hash(pub Vec<u8>);
    #[derive(Debug, Eq, PartialEq)]
    pub(crate) struct State(pub Vec<u8>);

    pub(crate) fn hash_snapshot(slice: &[u8]) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(slice);
        hasher.finalize().to_vec()
    }

    impl State {
        pub fn len(&self) -> usize {
            self.0.len()
        }

        pub fn hash(&self) -> Hash {
            Hash(hash_snapshot(&self.0))
        }
    }

    impl std::ops::Deref for Hash {
        type Target = [u8];

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }
}
