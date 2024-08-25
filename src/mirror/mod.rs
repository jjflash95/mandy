//! Defines the config and server layer

mod actions;
pub(crate) mod helpers;
mod tests;

use bytes::Bytes;
use thiserror::Error;
use uuid::Uuid;

use crate::mirror::helpers::ParameterArray;
use crate::net::{
    AsyncStream, Connection, InstanceRole, Mode, Node, SharedNode, SyncedConnectionPacket,
};
use crate::rsmp::{ParseError, Rsmp};
use crate::store::{self, SharedStore, Storage};
use crate::{debug, error, info, log_enabled, trace, warn};
use SyncSignal::*;

use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::{fmt::Debug, net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, RwLock};

pub(crate) type NodeRef = Arc<SharedNode>;

#[macro_export]
macro_rules! vec_from {
    ($($slice:expr),+) => {
        {
            let mut out = vec![];
            (
                $(
                    out.extend_from_slice($slice),
                )*
            );
            out
        }
    }
}

#[macro_export]
macro_rules! attempt {
    ($conn:expr, $block:block) => {
        match (async { $block }).await {
            Ok(val) => Ok(val),
            Err(err) => {
                $conn.write(&Rsmp::from(&err).to_bytes()).await?;
                Err(err)
            }
        }
    };
}

#[derive(Debug, Error)]
pub(crate) enum HandleError {
    #[error("{0}")]
    Io(String),
    #[error("Failed to read stream: {0}")]
    ReadStream(#[from] tokio::io::Error),
    #[error("Failed to parse message: {0}")]
    ParseMessage(#[from] crate::rsmp::ParseError),
    #[error("Failed to execute command: {0}")]
    InvalidCommand(#[from] crate::command::CommandError),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("Missing command parameter {1} at index: {0}")]
    MissingParameter(usize, &'static str),
    #[error("Malformed command parameter {1} at index: {0}")]
    MalformedParameter(usize, &'static str),
    #[error("{0}")]
    Sync(String),
    #[error("Failed to serialize store: {0}")]
    Serialize(String),
    #[error("Failed to serialize store: {0}")]
    Deserialize(String),
    #[error("Write Error: {0:?}")]
    Write(store::Error),
    #[error("Read Error: {0:?}")]
    Read(store::Error),
    #[error("{0}")]
    InvalidOp(&'static str),
}

/// Returns an initialized mirror for the given store and config
pub fn init<Store: Storage>(
    config: Config,
    store: Store,
) -> impl Future<Output = Result<Mirror<Store, Init>, InitError>> {
    Mirror::init(config, store)
}

/// A config type to initialize a mirror
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub uuid: Uuid,
    pub addr: SocketAddr,
    pub role: Role,
}

impl Config {
    /// Creates a new config, if no uuid is provided a random one will be generated
    pub fn new(uuid: Option<Uuid>, addr: SocketAddr, role: Role) -> Self {
        Self {
            uuid: uuid.unwrap_or_else(Uuid::new_v4),
            addr,
            role,
        }
    }
}

/// An enum defining if the instance should act as `Master` or `Replica`
#[derive(Debug, Clone, Copy)]
pub enum Role {
    Master,
    Replica(
        /// Remote instance's ip address
        SocketAddr,
    ),
}

/// Marker type for an uninitialized instance
#[derive(Debug)]
pub struct Uninit;

/// Marker type for an initialized instance
#[derive(Debug)]
pub struct Init;

/// An instance of a server acting as either `Master` or `Replica`.
///
/// A `Master` instance will act as a `WRITE` and `READ` unit and won't have any parents.
///
/// A `Replica` on initialization will atempt to subscribe to a `Parent` to init it's own storage
/// and get a channel to accept incoming updates from said parent.
///
/// Both `Master` and `Replica`s can act as parents, the only difference is that only a `Master`
/// instance can execute direct `WRITE`s from an incoming connection, the `Replica` is just limited
/// to all other commands except `WRITE`. The only case where the `Replica`'s internal state will change
/// is when its `Parent` broadcasts an update (which can be `WRITE`, `REDIRECT` or `PROMOTE`)
///
/// # Example
///
/// ```
/// use mandy::store::{Error, Input, Output, Readable, Sendable, Writable};
/// use mandy::{Bytes, Config, Connection, Mirror, Role, command};
/// use mandy::rsmp::*;
/// use std::net::SocketAddr;
/// use std::time::Duration;
///
/// #[derive(Debug, PartialEq, Clone, Copy)]
/// pub enum Fruit {
///     Apple,
///     Banana,
///     Peach,
/// }
///
/// #[derive(Debug, Default, PartialEq)]
/// pub struct Basket {
///     pub contents: Vec<Fruit>,
/// }
///
/// // Input is a fruit kind and the output is the amount of those we have stored
/// impl Readable for Basket {
///     type Input = Fruit;
///     type Output = usize;
///
///     fn read(&self, query: Vec<Self::Input>) -> Result<Self::Output, Error> {
///         let target = query[0];
///
///         Ok(self
///             .contents
///             .iter()
///             .filter(|fruit| &target == *fruit)
///             .count())
///     }
/// }
///
/// // We add the input and return the length of the contents
/// impl Writable for Basket {
///     type Input = Fruit;
///     type Output = usize;
///
///     fn write(&mut self, query: Vec<Self::Input>) -> Result<Self::Output, Error> {
///         self.contents.extend(query);
///         Ok(self.contents.len())
///     }
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
/// async fn send_fruit(remote: SocketAddr, fruit: &str) -> String {
///     send(remote, command!["WRITE", fruit]).await
/// }
///
/// async fn get_fruit_amount(remote: SocketAddr, fruit: &str) -> String {
///     send(remote, command!["READ", fruit]).await
/// }
///
/// #[tokio::main]
/// async fn main() {
///     let master: SocketAddr = "127.0.0.1:8080".parse().unwrap();
///     let master_cfg = Config::new(None, master, Role::Master);
///
///     let replica: SocketAddr = "127.0.0.1:8081".parse().unwrap();
///     let replica_cfg = Config::new(None, replica, Role::Replica(master));
///
///     let another_replica: SocketAddr = "127.0.0.1:8082".parse().unwrap();
///     let another_replica_cfg = Config::new(None, another_replica, Role::Replica(replica));
///
///     let master_instance = Mirror::init(master_cfg, Basket::default()).await.unwrap();
///
///     tokio::spawn(async move {
///         master_instance.serve().await.unwrap();
///     });
///
///     tokio::time::sleep(Duration::from_millis(1000)).await;
///
///     // Send fruits to master instance
///     assert_eq!("1", &send_fruit(master, "🍎").await);
///     assert_eq!("2", &send_fruit(master, "🍎").await);
///     assert_eq!("3", &send_fruit(master, "🍎").await);
///     assert_eq!("4", &send_fruit(master, "🍌").await);
///
///     // Get fruit amounts by kind from master
///     assert_eq!("1", &get_fruit_amount(master, "🍌").await);
///     assert_eq!("3", &get_fruit_amount(master, "🍎").await);
///     assert_eq!("0", &get_fruit_amount(master, "🍑").await);
///
///     // Spin up replica pointing to master
///     let replica_instance = Mirror::init(replica_cfg, Basket::default()).await.unwrap();
///
///     tokio::spawn(async move {
///         replica_instance.serve().await.unwrap();
///     });
///
///     tokio::time::sleep(Duration::from_millis(1000)).await;
///
///     // After initialization check that the state is correct
///     assert_eq!("1", &get_fruit_amount(replica, "🍌").await);
///     assert_eq!("3", &get_fruit_amount(replica, "🍎").await);
///     assert_eq!("0", &get_fruit_amount(replica, "🍑").await);
///
///     // Spin another replica instance, this time pointing to the second replica
///     let another_replica_instance = Mirror::init(another_replica_cfg, Basket::default()).await.unwrap();
///
///     tokio::spawn(async move {
///         another_replica_instance.serve().await.unwrap();
///     });
///
///     tokio::time::sleep(Duration::from_millis(1000)).await;
///
///     // Send a another fruit to the master and wait for it to trickle down
///     // Master -> Replica1 -> Replica2
///     assert_eq!("5", &send_fruit(master, "🍎").await);
///
///     tokio::time::sleep(Duration::from_millis(1000)).await;
///
///     // After the delay check that the state in the second replica is the same as in master
///     assert_eq!("1", &get_fruit_amount(another_replica, "🍌").await);
///     assert_eq!("4", &get_fruit_amount(another_replica, "🍎").await);
///     assert_eq!("0", &get_fruit_amount(another_replica, "🍑").await);
/// }
///
/// // *********************************************************************
/// // Utility functions and other trait impls
/// // Not relevant for this example
/// // *********************************************************************
///
/// async fn send(remote: SocketAddr, command: Rsmp) -> String {
///     let mut buf = [0; 512];
///     let mut writer = Connection::from_addr(remote)
///         .await
///         .unwrap();
///
///     writer.write(&command.to_bytes()).await.unwrap();
///
///     let read_bytes = writer.read(&mut buf).await.unwrap();
///
///     std::str::from_utf8(&buf[..read_bytes]).unwrap().to_string()
/// }
///
/// impl Input for Fruit {
///     fn from_bytes(bytes: &Bytes) -> Result<Self, mandy::store::Error>
///     where
///         Self: Sized,
///     {
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
/// ```
#[derive(Debug)]
pub struct Mirror<Store, State = Uninit> {
    node: NodeRef,
    store: Arc<RwLock<Store>>,
    state: PhantomData<State>,
}

#[derive(Debug)]
pub enum InitError {
    /// Remote refused to establish initial connection
    RemoteRefused,
    /// Remote accepted the initial connection but some error happened during the sync process.
    /// This error is usually related to the store serialization/deserialization.
    Sync(String),
}

#[derive(Debug)]
enum SyncSignal {
    Dropped,
    Noop,
    Promoted(Uuid, SocketAddr),
    Redirected,
}

impl<Store: Storage> Mirror<Store, Uninit> {
    /// Spins up a new mirror with the given config.
    /// If `Role::Master` this operation is infallible, otherwise the mirror will attempt to connect to the master and execute a full sync.
    pub async fn init(config: Config, store: Store) -> Result<Mirror<Store, Init>, InitError> {
        let store = SharedStore::new(RwLock::new(store));
        let instance = match config.role {
            Role::Master => {
                let node = Node::new(config.uuid, InstanceRole::Master, config.addr);
                SharedNode::new(node)
            }
            Role::Replica(remote_addr) => {
                let mut remote = Connection::from_addr(remote_addr).await?;
                let remote_uuid =
                    get_master_state(&mut remote, config.uuid, config.addr, store.clone()).await?;
                let role = InstanceRole::Replica(Mode::Child(remote_uuid, remote_addr));
                let node = Node::new(config.uuid, role, config.addr);
                let shared = SharedNode::new(node);
                init_sync_routine(remote, shared.clone(), store.clone());
                shared
            }
        };

        Ok(Mirror::new(instance, store))
    }

    fn new(node: NodeRef, store: SharedStore<Store>) -> Mirror<Store, Init> {
        Mirror {
            node,
            store,
            state: PhantomData,
        }
    }
}

impl<Store> Mirror<Store, Init>
where
    Store: Storage,
{
    /// Starts listening and handling incoming connections.
    /// This method will block until the node is shutdown remotely.
    pub async fn serve(&self) -> io::Result<()> {
        let (sender, shutdown) = oneshot::channel();
        let node = &self.node;
        let listener = TcpListener::bind(node.addr().await).await?;
        node.set_shutdown(sender).await;

        tokio::select! {
            _ = shutdown => (),
            _ = self.serve_inner(listener) => (),
        }

        Ok(())
    }

    async fn serve_inner(&self, listener: TcpListener) {
        info!(target: "startup", "{:?} Node started [{}]", self.node.role().await, self.node.uuid().await);
        loop {
            if let Ok((conn, addy)) = listener.accept().await {
                debug!(target: "connections", "Accepted connection from: {addy}");
                self.handle(Connection::new(conn));
            }
        }
    }

    pub(crate) fn handle<S: AsyncStream>(&self, conn: Connection<S>) {
        let (node, store) = (self.node.clone(), self.store.clone());
        tokio::spawn(async move {
            handle_conn(conn, node, store)
                .await
                .inspect_err(|e| warn!(target: "connections", "Handling connection: {e}"))
        });
    }
}

pub(crate) fn init_sync_routine<Stream, Store>(
    mut conn: Connection<Stream>,
    node: NodeRef,
    store: SharedStore<Store>,
) where
    Stream: AsyncStream,
    Store: Storage,
{
    tokio::spawn(async move {
        loop {
            match handle_sync_conn(&mut conn, node.clone(), store.clone())
                .await
                .inspect_err(|e| error!("Failed processing msg: {e}"))
            {
                Ok(Promoted(child_uuid, child_addy)) => {
                    SyncedConnectionPacket::starting_now(child_uuid, child_addy, conn, node, store)
                        .dispatch()
                        .await;
                    return;
                }
                Ok(Dropped) => {
                    node.orphaned().await;
                    error!(target: "sync", "remote closed sync connection, running like a headless chicken...");
                    return;
                }
                Ok(Redirected) => todo!(),
                _ => continue,
            }
        }
    });
}

async fn handle_sync_conn<'a, Stream, Store>(
    conn: &mut Connection<Stream>,
    node: NodeRef,
    store: SharedStore<Store>,
) -> Result<SyncSignal, HandleError>
where
    Stream: AsyncStream,
    Store: Storage,
{
    use crate::command::Command;
    use crate::command::CommandKind::*;

    let mut buf = [0; 512];
    match conn.read(&mut buf).await {
        Ok(0) => return Ok(Dropped),
        Ok(_) => {}
        Err(e) => Err(e)?,
    }

    let msg = parse_rsmp(&buf).await?;
    let cmd = Command::from_rsmp(msg)?;

    if log_enabled!(log::Level::Trace) {
        trace!(target: "sync",
            "Received command from ({}): {:?}",
            &node.addr().await,
            &cmd
        );
    }

    match &cmd.kind {
        Promote => {
            let ip = conn.addy()?.ip();
            let uuid = cmd.args().deserialize(0, "UUID")?;
            let port = cmd.args().deserialize(1, "PORT")?;
            conn.write(&crate::rsmp::ACK.to_bytes()).await?;
            actions::sync_promote(cmd.args(), node, store).await;
            Ok(Promoted(uuid, (ip, port).into()))
        }
        Write => {
            actions::sync_write(&cmd.args, node, store).await?;
            Ok(Noop)
        }
        Redirect => {
            actions::sync_redirect(&cmd.args, node, store).await?;
            Ok(Redirected)
        }
        _ => Err(HandleError::InvalidOp("Unexpected SyncCommand")),
    }
}

async fn get_master_state<'cmd, Stream, Store>(
    remote: &mut Connection<Stream>,
    this_uuid: Uuid,
    this_addr: SocketAddr,
    store: SharedStore<Store>,
) -> Result<Uuid, HandleError>
where
    Stream: AsyncStream,
    Store: Storage,
{
    info!(target: "sync", "Syncing to master: {:?}", this_addr);
    let (master_uuid, full_state) = actions::get_subscription(this_uuid, this_addr, remote).await?;
    info!(target: "sync", "Got subscription and store from remote");

    if let Some((expected_hash, state)) = full_state {
        let total_bytes = state.len();
        let computed_hash = state.hash();
        if computed_hash != expected_hash {
            return Err(HandleError::Sync(format!(
                "Fullsync error, mismatched hashes {}, expected({})",
                hex::encode(&*computed_hash),
                hex::encode(&*expected_hash)
            )));
        }
        info!(target: "connections", "Got store successfully [{}] ({total_bytes}) bytes", hex::encode(&*computed_hash));

        store
            .write()
            .await
            .initialize(state.0)
            .map_err(|e| HandleError::Deserialize((*e).to_string()))?;
    }

    info!(target: "sync", "Store initialized");
    Ok(master_uuid)
}

async fn sync_to_master<'cmd, Stream, Store>(
    mut remote: Connection<Stream>,
    node: NodeRef,
    store: SharedStore<Store>,
) -> Result<Uuid, HandleError>
where
    Stream: AsyncStream,
    Store: Storage,
{
    let remote_addr = remote
        .addy()
        .map_err(|e| HandleError::Io(format!("Could not get remote address: {e}")))?;
    let addr = node.addr().await;
    let uuid = node.uuid().await;

    let master_uuid = get_master_state(&mut remote, uuid, addr, store.clone()).await?;
    init_sync_routine(remote, node.clone(), store);
    node.adopted(master_uuid, remote_addr).await;
    Ok(master_uuid)
}

async fn handle_conn<'a, Stream, Store>(
    mut conn: Connection<Stream>,
    node: NodeRef,
    store: SharedStore<Store>,
) -> Result<(), HandleError>
where
    Stream: AsyncStream,
    Store: Storage,
{
    use crate::command::Command;
    use crate::command::CommandKind::*;

    let mut buf = [0; 512];
    if let 0 = conn.read(&mut buf).await? {
        return Err(HandleError::Io("Connection closed unexpectedly".into()));
    }

    let msg = attempt!(&mut conn, {
        parse_rsmp(&buf).await.map_err(Into::<HandleError>::into)
    })?;

    let cmd = attempt!(&mut conn, { Command::from_rsmp(msg) })?;

    if log_enabled!(log::Level::Trace) {
        trace!(
            target: "connections",
            "Received command from ({}): {:?}",
            node.addr().await,
            &cmd
        );
    }

    match (node.role().await, &cmd.kind) {
        (InstanceRole::Master, Promote) => actions::promote(cmd.args(), conn, node, store).await,
        (InstanceRole::Master, Revive) => {
            actions::unallowed("Can't perform REVIVE on master", conn, node, store).await
        }
        (InstanceRole::Replica(..), Write) => {
            actions::unallowed("Can't perform WRITE on replica", conn, node, store).await
        }
        (InstanceRole::Replica(..), Adopt) => actions::adopt(cmd.args(), conn, node, store).await,
        (InstanceRole::Replica(..), Promote) => {
            actions::repl_promote(cmd.args(), conn, node, store).await
        }
        (InstanceRole::Master, Write) => actions::write(cmd.args(), conn, node, store).await,
        (_, Ping) => actions::ping(cmd.args(), conn, node, store).await,
        (_, ConfGet) => actions::confget(cmd.args(), conn, node, store).await,
        (_, Read) => actions::read(cmd.args(), conn, node, store).await,
        (_, Redirect) => actions::redirect(cmd.args(), conn, node, store).await,
        (_, Adopt) => actions::adopt(cmd.args(), conn, node, store).await,
        (_, Revive) => actions::revive(cmd.args(), conn, node, store).await,
        (_, Subscribe) => actions::subscribe(cmd.args(), conn, node, store).await,
        (_, Shutdown) => actions::shutdown(cmd.args(), conn, node, store).await,
    }
}

async fn parse_rsmp(buf: &[u8]) -> Result<Rsmp, ParseError> {
    Rsmp::parse_bytes(buf)
}

impl From<&HandleError> for Rsmp {
    fn from(value: &HandleError) -> Self {
        Rsmp::Error(Bytes::copy_from_slice(value.to_string().as_bytes()))
    }
}

impl From<HandleError> for InitError {
    fn from(value: HandleError) -> Self {
        match value {
            HandleError::Deserialize(e) => InitError::Sync(e.to_string()),
            HandleError::Sync(e) => InitError::Sync(e),
            HandleError::Io(e) => InitError::Sync(e),
            HandleError::ReadStream(e) => InitError::Sync(e.to_string()),
            _ => unreachable!(),
        }
    }
}

impl From<io::Error> for InitError {
    fn from(_: io::Error) -> Self {
        InitError::RemoteRefused
    }
}
