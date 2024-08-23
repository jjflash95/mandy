#[cfg(test)]
pub mod tests;

pub(crate) use std::net::SocketAddr;
pub use tokio::net::TcpStream;

use crate::{
    command::{Command, CommandKind}, log_enabled, mirror::{init_sync_routine, NodeRef}, rsmp::{self, Rsmp}, store::{SharedStore, Storage}, trace
};

use futures::{lock::Mutex, Future};
use log::{error, info};
use std::{
    collections::VecDeque,
    fmt::{Debug, Display, Formatter},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    select,
    sync::{
        mpsc::{self, error::SendError},
        oneshot, RwLock, RwLockReadGuard, RwLockWriteGuard,
    },
    time::sleep,
};
use uuid::Uuid;

pub static DEFAULT_NODE_ADDR_STR: &str = "127.0.0.1:6111";
pub static DEFAULT_SHUTDOWN_SLEEP_MS: usize = 5000;

static MAX_DBG_MESSAGE_LEN: usize = 1023;
static COMMAND_MAX_RETRIES: usize = 3;
static PROMOTE_TIMEOUT_MS: u64 = 10_000;
static NO_COMMANDS_SLEEP_MS: u64 = 10;
static DEFAULT_CMD_BUFFER_MAX_CAPACITY: usize = 100;


pub type Port = u16;

pub type TimeStampMillis = u128;

type ExtPromoteResult = Result<(), String>;

pub trait AsyncStream:
    Debug + Addy + AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static
{
}

pub trait Addy {
    fn addr(&self) -> io::Result<SocketAddr>;
}

/// This handles the external replicas to have their state up to date...
/// For every new subscriber, we ge the lock to the store, serialize the database
/// and send the bytes via TCP, however in the meantime we could be handling
/// new write operations and the replica would not know of them, so instead we create
/// a sync packet with the timestamp marking the last update of the store,
/// then we pull all the commands that came after that timestamp from the node's
/// CommandBuffer and send them back to the new subscriber in order.
/// After that we just send the operations in as they arrive
pub(crate) struct SyncedConnectionPacket<Stream, Store>
where
    Stream: AsyncStream,
    Store: Storage,
{
    pub uuid: Uuid,
    pub addy: SocketAddr,
    pub conn: Connection<Stream>,
    pub node: NodeRef,
    pub store: SharedStore<Store>,
    pub snapshot_ts: TimeStampMillis,
}

#[derive(Debug)]
pub(crate) struct CommandBuffer {
    max_capacity: usize,
    inner: Arc<RwLock<VecDeque<(TimeStampMillis, Command)>>>,
}

#[derive(Debug)]
pub(crate) struct SharedNode {
    pub cmd_buf: CommandBuffer,
    node: Arc<RwLock<Node>>,
}

impl SharedNode {
    pub fn new(node: Node) -> Arc<Self> {
        let node = Self {
            node: Arc::new(RwLock::new(node)),
            cmd_buf: CommandBuffer::with_capacity(DEFAULT_CMD_BUFFER_MAX_CAPACITY),
        };

        Arc::new(node)
    }

    pub async fn role(&self) -> InstanceRole {
        self.node.read().await.role()
    }

    pub async fn uuid(&self) -> Uuid {
        self.node.read().await.uuid()
    }

    pub async fn addr(&self) -> SocketAddr {
        self.node.read().await.addr()
    }

    pub async fn promote(&self, role: InstanceRole) {
        self.write().await.promote(role)
    }

    pub(crate) async fn adopted(&self, uuid: Uuid, addr: SocketAddr) {
        self.write().await.adopted(uuid, addr)
    }

    pub(crate) async fn orphaned(&self) {
        self.write().await.orphaned()
    }

    pub(crate) async fn broadcast(&self, cmd: Command) {
        self.cmd_buf.push(cmd.clone()).await;
        self.write().await.broadcast(cmd).await
    }

    async fn write(&self) -> RwLockWriteGuard<Node> {
        self.node.write().await
    }

    pub async fn read(&self) -> RwLockReadGuard<Node> {
        self.node.read().await
    }

    pub(crate) async fn set_shutdown(&self, sender: oneshot::Sender<()>) {
        self.write().await.set_shutdown(sender)
    }

    pub(crate) async fn shutdown(&self) -> Result<(), ()> {
        self.write().await.shutdown()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Mode {
    Orphan,
    Child(Uuid, SocketAddr)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InstanceRole {
    Master,
    Replica(Mode),
}

#[derive(Debug)]
pub(crate) struct Node {
    uuid: Uuid,
    role: InstanceRole,
    addr: SocketAddr,
    replicas: RwLock<Vec<Replica>>,
    shutdown: Option<oneshot::Sender<()>>,
}

#[derive(Debug)]
pub(crate) struct Replica {
    pub uuid: Uuid,
    // Parsed address of the replica, {ip, listening_port}...
    // Though they look similar, this is not the actual port of the connection
    // being used to send commands between this instance and the replica, this instead
    // is the port that the replica uses to listen to incoming connections, in other
    // words the `entrypoint` of the replica
    pub addy: SocketAddr,
    // Channel used to send commands from instance => replica
    tx: mpsc::Sender<Command>,
    // Channel used by instance to check that promotion was successful
    // we don't use oneshot so we can retry if the replica fails to promote
    // or so we don't have to drop the replica if it does not ACK the promotion
    //
    // Since signal.recv() takes &mut reference, we use Mutex to allow a &Replica
    // to use signal without having to use the write lock in node.replicas_mut()
    signal: Mutex<mpsc::Receiver<ExtPromoteResult>>,
}

/// A wrapper for TcpStream
#[derive(Debug)]
pub struct Connection<S: AsyncStream> {
    stream: S,
}

impl<Stream, Store> SyncedConnectionPacket<Stream, Store>
where
    Stream: AsyncStream,
    Store: Storage,
{
    pub fn new(
        uuid: Uuid,
        addy: SocketAddr,
        conn: Connection<Stream>,
        node: NodeRef,
        store: SharedStore<Store>,
        snapshot_ts: TimeStampMillis,
    ) -> Self {
        Self {
            uuid,
            addy,
            conn,
            node,
            store,
            snapshot_ts,
        }
    }

    pub fn starting_now(
        uuid: Uuid,
        addy: SocketAddr,
        conn: Connection<Stream>,
        node: NodeRef,
        store: SharedStore<Store>,
    ) -> Self {
        Self::new(uuid, addy, conn, node, store, ts_millis())
    }

    /// Dispatches a thread that sends mutations to the specified replica node
    ///
    /// We avoid using the lock until the replica is created and the sync thread is dispatched.
    /// * `(uuid, ip, port)` - Replica node information
    /// * `conn` - Connection(Stream) with replica
    /// * `node` - Reference to the Arc wrapped self
    /// * `store` - Shared store
    pub async fn dispatch(self) {
        let (uuid, addy) = (self.uuid, self.addy);
        let node = self.node.clone();

        let (ctx, prx) = dispatch_sync_thread::<Stream, Store>(self);

        node.write()
            .await
            .add_replica(Replica::new(uuid, addy, ctx, prx))
            .await;

        info!(target: "connections", "Added [{uuid}] to replica list");
    }
}

impl CommandBuffer {
    pub fn with_capacity(n: usize) -> Self {
        Self {
            max_capacity: n,
            inner: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    pub async fn push(&self, cmd: Command) {
        let now = ts_millis();
        let mut buf_lock = self.inner.write().await;
        if buf_lock.capacity() >= self.max_capacity {
            buf_lock.pop_front();
        }
        buf_lock.push_back((now, cmd));
    }

    pub async fn supports_partial_sync(&self, since: TimeStampMillis) -> bool {
        self.inner
            .read()
            .await
            .iter()
            .any(|(ts, _)| ts <= &since)
    }

    pub async fn clone_since(
        &self,
        since: TimeStampMillis,
    ) -> VecDeque<(TimeStampMillis, Command)> {
        self.inner
            .read()
            .await
            .iter()
            .filter(|(ts, _)| ts >= &since)
            .cloned()
            .collect()
    }
}

impl std::default::Default for Node {
    fn default() -> Self {
        Self::new(Uuid::new_v4(), InstanceRole::Master, DEFAULT_NODE_ADDR_STR.parse().unwrap())
    }
}

impl Node {
    pub fn new(uuid: Uuid, role: InstanceRole, addr: SocketAddr) -> Self {
        Self {
            uuid,
            role,
            addr,
            replicas: RwLock::new(vec![]),
            shutdown: None,
        }
    }

    pub fn orphaned(&mut self) {
        self.role = InstanceRole::Replica(Mode::Orphan)
    }

    pub fn adopted(&mut self, uuid: Uuid, addr: SocketAddr) {
        self.role = InstanceRole::Replica(Mode::Child(uuid, addr))
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn role(&self) -> InstanceRole {
        self.role
    }

    pub fn promote(&mut self, role: InstanceRole) {
        self.role = role
    }

    pub async fn replcount(&self) -> usize {
        self.replicas.read().await.len()
    }

    pub(crate) fn shutdown(&mut self) -> Result<(), ()> {
        self.shutdown.take().unwrap().send(())
    }

    pub(crate) fn set_shutdown(&mut self, sender: oneshot::Sender<()>) {
        self.shutdown = Some(sender)
    }

    pub(crate) async fn broadcast(&self, cmd: Command) {
        for replica in self.replicas.write().await.iter_mut() {
            let _ = replica.send(cmd.clone()).await.inspect_err(|SendError(e)| {
                error!("{}. Failed to broadcast message: {:?}", replica.uuid, e)
            });
        }
    }

    pub(crate) fn replicas(&self) -> impl Future<Output = RwLockReadGuard<Vec<Replica>>> {
        self.replicas.read()
    }

    pub(crate) fn replicas_mut(&self) -> impl Future<Output = RwLockWriteGuard<Vec<Replica>>> {
        self.replicas.write()
    }

    pub(crate) async fn add_replica(&mut self, replica: Replica) {
        self.replicas.write().await.push(replica)
    }

    pub(crate) async fn rm_replica(&mut self, uuid: Uuid) {
        self.replicas.write().await.retain(|r| r.uuid != uuid)
    }
}

impl Replica {
    pub fn new(
        uuid: Uuid,
        addy: SocketAddr,
        tx: mpsc::Sender<Command>,
        signal: mpsc::Receiver<ExtPromoteResult>,
    ) -> Self {
        Self {
            uuid,
            addy,
            tx,
            signal: signal.into(),
        }
    }

    pub fn send(&self, cmd: Command) -> impl Future<Output = Result<(), SendError<Command>>> + '_ {
        self.tx.send(cmd)
    }

    pub async fn wait_promoted_signal(&self) -> Result<(), String> {
        let mut signal = self.signal.lock().await;
        select! {
            res = signal.recv() => {
                match res {
                    Some(Ok(_)) => Ok(()),
                    Some(Err(e)) => Err(format!("Failed to promote replica: {e}"))?,
                    None => Err("Promote channel closed, this code should be unreachable, sorry :^(".to_string())
                }
            },
            _ = sleep(Duration::from_millis(PROMOTE_TIMEOUT_MS)) => Err("Timeout waiting for promotion signal".to_string())
        }
    }
}

impl<S> Connection<S>
where
    S: AsyncStream,
{
    pub fn new(stream: S) -> Self {
        Self { stream }
    }

    pub fn addy(&self) -> io::Result<SocketAddr> {
        self.stream.addr()
    }
}

impl Connection<TcpStream> {
    pub async fn from_addr(addr: SocketAddr) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self::new(stream))
    }
}

impl<S: AsyncStream> Connection<S> {
    pub async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.stream.read(buf).await {
            Ok(b) => {
                if log_enabled!(log::Level::Trace) {
                    let addr = self
                        .addy()
                        .map(|a| a.to_string())
                        .unwrap_or_else(|_| "UNKNOWN".to_string());

                    let msg = &buf[..b];
                    let repr = if msg.len() > MAX_DBG_MESSAGE_LEN {
                        format!("[{}] bytes...", msg.len()).into()
                    } else {
                        String::from_utf8_lossy(msg)
                    };

                    trace!(
                        target: "incoming",
                        "{} <- {:?}",
                        addr,
                        repr.trim_end_matches('\0')
                    );
                }
                Ok(b)
            }
            Err(e) => Err(Into::into(e)),
        }
    }

    pub async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.stream.write(buf).await {
            Ok(b) => {
                if log_enabled!(log::Level::Trace) {
                    let addr = self
                        .addy()
                        .map(|a| a.to_string())
                        .unwrap_or_else(|_| "UNKNOWN".to_string());

                    let msg = &buf[..b];
                    let repr = if msg.len() > MAX_DBG_MESSAGE_LEN {
                        format!("[{}] bytes...", msg.len()).into()
                    } else {
                        String::from_utf8_lossy(msg)
                    };

                    trace!(
                        target: "outgoing",
                        "{} -> {:?}",
                        addr,
                        repr.trim_end_matches('\0')
                    );
                }
                Ok(b)
            }
            Err(e) => Err(Into::into(e)),
        }
    }
}

impl Display for Mode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Mode::Orphan => "ORPHAN".to_string(),
            Mode::Child(uuid, addr) => format!("CHILD OF ({uuid}, {addr})")
        };
        write!(f, "{s}")
    }
}

impl Display for InstanceRole {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            InstanceRole::Master => "MASTER".to_string(),
            InstanceRole::Replica(addr) => format!("REPLICA {}", addr),
        };
        write!(f, "{s}")
    }
}

impl Addy for TcpStream {
    fn addr(&self) -> io::Result<SocketAddr> {
        self.peer_addr()
    }
}

impl<T> AsyncStream for T where
    T: Debug + Addy + AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static
{
}

fn dispatch_sync_thread<S, Store>(
    packet: SyncedConnectionPacket<S, Store>,
) -> (mpsc::Sender<Command>, mpsc::Receiver<ExtPromoteResult>)
where
    S: AsyncStream,
    Store: Storage,
{
    async fn send_promote_ext<S: AsyncStream>(
        conn: &mut Connection<S>,
        buf: &[u8],
    ) -> Result<(), String> {
        let mut response = [0; 1024];

        let _ = conn
            .write(buf)
            .await
            .map_err(|e| format!("Failed to write to remote: {:?}", e))?;

        let _ = conn
            .read(&mut response)
            .await
            .map_err(|e| format!("Remote never responded promote: {:?}", e))?;

        let msg = Rsmp::parse_bytes(&response)
            .map_err(|e| format!("Failed to parse remote response: {:?}", e))?;

        if !rsmp::is_ack(&msg) {
            return Err(format!("Remote didn't ACK: {:?}", msg));
        }
        Ok(())
    }

    let (ctx, mut crx) = mpsc::channel(32);
    let (ptx, prx) = mpsc::channel(1);

    tokio::spawn(async move {
        let SyncedConnectionPacket {
            mut conn,
            node,
            store,
            snapshot_ts,
            ..
        } = packet;

        let target = format!("sync => {}", &packet.uuid.to_string()[..8]);
        let cmd_queue = node.cmd_buf.clone_since(snapshot_ts).await;
        let cmd_queue: VecDeque<(usize, Command)> =
            cmd_queue.into_iter().map(|(_, cmd)| (0, cmd)).collect();

        let consumer = Arc::new(Mutex::new(cmd_queue));
        let producer = Arc::clone(&consumer);

        let producer = async move {
            while let Some(cmd) = crx.recv().await {
                producer.lock().await.push_back((0, cmd));
            }
        };

        let consumer = async move {
            loop {
                // ensure that sleep between loops is smaller than any sleep
                // used for testing, also avoid using the lock in a match statement
                // i.e. match consumer.lock().await.pop_front() { ... }

                // TODO: handle conn.write errors gracefully
                let next = { consumer.lock().await.pop_front() };
                let Some(pack) = next else {
                    sleep(Duration::from_millis(NO_COMMANDS_SLEEP_MS)).await;
                    continue;
                };

                let (attempts, command) = pack;

                if attempts >= COMMAND_MAX_RETRIES {
                    error!(target: &target, "Failed to send command to replica more than *{COMMAND_MAX_RETRIES} times... exiting thread");
                    node.write().await.rm_replica(packet.uuid).await;
                    return;
                }

                match command.kind {
                    CommandKind::Write => {
                        if let Err(e) = conn.write(&Rsmp::from(&command).to_bytes()).await {
                            // If a command fails we push it back to the front of the queue
                            error!(target: &target, "Failed to propagate command: {e}");
                            consumer.lock().await.push_front((attempts + 1, command));
                        }
                    }
                    CommandKind::Promote => {
                        // {ptx:promotion_transmitter} failing to send is an irrecoverable error, we would
                        // have 2 instances working as master
                        // Also. this task is supposed to update the role and dispatch
                        // the sync routine, because is the only one with the connection
                        // in scope

                        let ext_res =
                            send_promote_ext(&mut conn, &Rsmp::from(&command).to_bytes()).await;
                        ptx.send(ext_res).await.unwrap();

                        let mut lock = node.write().await;
                        lock.role = InstanceRole::Replica(Mode::Child(packet.uuid, packet.addy));
                        lock.rm_replica(packet.uuid).await;
                        drop(lock);

                        init_sync_routine(conn, node.clone(), store);
                        return;
                    }
                    CommandKind::Shutdown => {
                        let _ = conn.write(&[]).await.inspect_err(|_| error!("Remote shutdown failed"));
                        return;
                    }
                    CommandKind::Redirect => todo!(),
                    _ => unreachable!(),
                };
            }
        };

        select! {
            _ = producer => (),
            _ = consumer => (),
        }
    });

    (ctx, prx)
}

pub(crate) fn ts_millis() -> TimeStampMillis {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}
