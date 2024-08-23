use crate::command::{Args, Command};
use crate::mirror::helpers::{AsDeserializedIter, DeserializedIter, ParameterArray};
use crate::mirror::{sync_to_master, HandleError, NodeRef, SharedStore};
use crate::net::{
    self, ts_millis, InstanceRole, AsyncStream, Connection, Mode, Port, SocketAddr, SyncedConnectionPacket,
    TimeStampMillis,
};
use crate::rsmp::{self, Rsmp, OK};
use crate::store::{self, utils, Output, Readable, Storage, Writable};
use crate::{args, expect_args, vec_from};
use crate::{attempt, debug, info, trace};

use bytes::Bytes;
use hex;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

type Subscription = (Uuid, Option<(utils::Hash, utils::State)>);

pub enum TransferState<'state> {
    Continue(&'state [u8]),
    End(&'state [u8]),
}

impl<'state> TransferState<'state> {
    pub fn slice(&self) -> &'state [u8] {
        match self {
            Self::Continue(s) => s,
            Self::End(s) => s,
        }
    }

    pub fn process_chunk(slice: &'state [u8]) -> Self {
        if let Some(position) = slice
            .windows(crate::rsmp::RAW_BYTES_END.len())
            .position(|w| w == crate::rsmp::RAW_BYTES_END)
        {
            return TransferState::End(&slice[0..position]);
        }
        TransferState::Continue(slice)
    }
}

pub async fn do_handshake<Stream>(
    uuid: Uuid,
    thisaddr: SocketAddr,
    remote: &mut Connection<Stream>,
) -> Result<Subscription, HandleError>
where
    Stream: AsyncStream,
{
    // Handshake format is &[{uuid_bytes:16}{FULL_SYNC_BYTES || PARTIAL_SYNC_BYTES}]
    // if FULL then we sent the full db in 1kb chunks until DBSTREAMEND footer

    let uuid = uuid.as_bytes();
    let port = thisaddr.port().to_be_bytes();
    let cmd = Command::subscribe(args!(uuid, &port));
    let _ = remote.write(&Rsmp::from(&cmd).to_bytes()).await?;

    let mut buf = [0; 1024];
    let b = remote.read(&mut buf).await?;

    let master_uuid = Uuid::from_slice(&buf[0..16])
        .map_err(|e| HandleError::Sync(format!("Handshake failed, uuid not provided: {e}")))?;

    match &buf[16..b] {
        crate::rsmp::RAW_FULL_SYNC => {
            remote.write(&rsmp::ACK.to_bytes()).await?;
            let mut hash = [0; 32];
            let _ = remote.read(&mut hash).await?;

            let mut db = vec![];
            let mut buf = [0; 1024];
            while let Ok(n) = remote.read(&mut buf).await {
                let processed = TransferState::process_chunk(&buf[0..n]);

                if n == 0 {
                    return Err(HandleError::Sync("Master closed connection".into()));
                };

                db.extend_from_slice(processed.slice());
                if matches!(processed, TransferState::End(_)) {
                    break;
                }
            }

            Ok((
                master_uuid,
                Some((utils::Hash(hash.to_vec()), utils::State(db))),
            ))
        }
        crate::rsmp::RAW_PARTIAL_SYNC => Ok((master_uuid, None)),
        _ => Err(HandleError::Sync(format!(
            "Unexpected master response, waiting for PARTIAL or FULL: {}",
            String::from_utf8_lossy(&buf[..b])
        ))),
    }
}

pub async fn get_subscription<Stream>(
    uuid: Uuid,
    thisaddr: SocketAddr,
    master: &mut Connection<Stream>,
) -> Result<Subscription, HandleError>
where
    Stream: AsyncStream,
{
    info!(target: "connections", "Attempting to get subscription to: {thisaddr}");
    do_handshake(uuid, thisaddr, master).await
}

async fn exec_write<Store>(
    node: NodeRef,
    store: SharedStore<Store>,
    input: Vec<<Store as Writable>::Input>,
) -> Result<<Store as Writable>::Output, HandleError>
where
    Store: Storage,
{
    if node.read().await.role() != InstanceRole::Master {
        Err(HandleError::InvalidOp(
            "REPLICA cannot execute WRITE operation",
        ))?;
    }

    exec_write_unchecked(store, input)
        .await
        .map_err(HandleError::Write)
}

async fn exec_write_unchecked<Store>(
    store: SharedStore<Store>,
    input: Vec<<Store as Writable>::Input>,
) -> Result<<Store as Writable>::Output, store::Error>
where
    Store: Storage,
{
    store.write().await.write(input)
}

async fn exec_read<Store>(
    _node: NodeRef,
    store: SharedStore<Store>,
    input: Vec<<Store as Readable>::Input>,
) -> Result<<Store as Readable>::Output, store::Error>
where
    Store: Storage,
{
    store.read().await.read(input)
}

pub(crate) trait FromCmd {
    fn from_cmd(cmd: Command) -> Self;
}

pub(crate) async fn sync_write<Store>(
    args: &Args,
    node: NodeRef,
    store: SharedStore<Store>,
) -> Result<(), HandleError>
where
    Store: Storage,
{
    let input = <Store as Writable>::parse_input(args).map_err(HandleError::Write)?;
    exec_write_unchecked(store, input)
        .await
        .map_err(HandleError::Write)?;

    // this needs to happen as soon as the write is made so the timestamp is correct
    node.broadcast(Command::write(Arc::clone(args))).await;

    Ok(())
}

pub(crate) async fn sync_redirect<Store>(
    args: &Args,
    node: NodeRef,
    store: SharedStore<Store>,
) -> Result<(), HandleError>
where
    Store: Storage,
{
    let addr = args.deserialize(0, "ADDR")?;
    let remote = Connection::from_addr(addr)
        .await
        .map_err(|e| HandleError::Sync(format!("Master refused to connect: {e:?}")))?;

    let master_uuid = sync_to_master(remote, node.clone(), store).await?;

    node.promote(net::InstanceRole::Replica(Mode::Child(master_uuid, addr)))
        .await;

    Ok(())
}

/// This should be infallible
pub(crate) async fn sync_promote<Store>(
    _: &Args,
    node: NodeRef,
    _: SharedStore<Store>,
)
where
    Store: Storage,
{
    node.promote(net::InstanceRole::Master).await;
}

pub(crate) async fn ping<Stream, Store>(
    _: &Args,
    mut conn: Connection<Stream>,
    _: NodeRef,
    _: SharedStore<Store>,
) -> Result<(), HandleError>
where
    Stream: AsyncStream,
    Store: Storage,
{
    conn.write(&rsmp::PONG.to_bytes()).await?;
    Ok(())
}

pub(crate) async fn write<Stream, Store>(
    args: &Args,
    mut conn: Connection<Stream>,
    node: NodeRef,
    store: SharedStore<Store>,
) -> Result<(), HandleError>
where
    Stream: AsyncStream,
    Store: Storage,
{
    let output = attempt!(&mut conn, {
        let input = <Store as Writable>::parse_input(args).map_err(HandleError::Write)?;
        exec_write(node.clone(), store, input).await
    })?;
    conn.write(&output.into_bytes()).await?;
    node.broadcast(Command::write(Arc::clone(args))).await;
    Ok(())
}

pub(crate) async fn unallowed<Stream, Store>(
    msg: &'static str,
    mut conn: Connection<Stream>,
    _: NodeRef,
    _: SharedStore<Store>,
) -> Result<(), HandleError>
where
    Stream: AsyncStream,
    Store: Storage,
{
    conn.write(&Rsmp::Error(msg.into()).to_bytes()).await?;
    Ok(())
}

pub(crate) async fn read<Stream, Store>(
    args: &Args,
    mut conn: Connection<Stream>,
    node: NodeRef,
    store: SharedStore<Store>,
) -> Result<(), HandleError>
where
    Stream: AsyncStream,
    Store: Storage,
{
    let output = attempt!(&mut conn, {
        let input = <Store as Readable>::parse_input(args).map_err(HandleError::Read)?;
        exec_read(node, store, input)
            .await
            .map_err(HandleError::Read)
    })?;

    conn.write(&output.into_bytes()).await?;
    Ok(())
}

pub(crate) async fn subscribe<Stream, Store>(
    args: &Args,
    mut conn: Connection<Stream>,
    node: NodeRef,
    store: SharedStore<Store>,
) -> Result<(), HandleError>
where
    Stream: AsyncStream,
    Store: Storage,
{
    // support for: SUBSCRIBE uuid port [timestamp] (optional) for partial subscription
    // if we at least one command in the CommandBuffer older than `timestamp` it means
    // we don't need a full resync and instead can dispatch a SyncPacket directly
    info!(target: "connections", "Attempting to process new subscriber: {}", conn.addy()?);

    let (uuid, addr, snapshot_ts) = attempt!(&mut conn, {
        let mut args = args.deser_iter();
        let uuid = args.next::<Uuid>()?;
        let port = args.next::<Port>()?;
        let addr = SocketAddr::new(conn.addy()?.ip(), port);
        let ts = args.next::<TimeStampMillis>().ok();
        Ok::<_, HandleError>((uuid, addr, ts))
    })?;

    if snapshot_ts.is_some()
        && node
            .cmd_buf
            .supports_partial_sync(*snapshot_ts.as_ref().unwrap())
            .await
    {
        conn.write(&vec_from!(
            &node.uuid().await.into_bytes(),
            crate::rsmp::RAW_PARTIAL_SYNC
        ))
        .await?;
        SyncedConnectionPacket::new(uuid, addr, conn, node, store, snapshot_ts.unwrap())
            .dispatch()
            .await;
    } else {
        conn.write(&vec_from!(
            &node.uuid().await.into_bytes(),
            crate::rsmp::RAW_FULL_SYNC
        ))
        .await?;
        let (snapshot_ts, state) = attempt!(&mut conn, {
            debug!(target: "connections", "Attempting to serialize store for: {uuid}");
            let (snapshot_ts, state) = attempt!(&mut conn, {
                let lock = store.read().await;
                let snapshot_ts = ts_millis();
                let state = lock.serialize().map_err(|e| HandleError::Serialize((*e).to_string()))?;
                drop(lock);
                Ok::<_, HandleError>((snapshot_ts, state))
            })?;

            debug!(target: "connections", "Store serialized successfully for: {uuid}");
            Ok::<_, HandleError>((snapshot_ts, state))
        })?;

        trace!(target: "connections", "Waiting for subscriber({uuid}) to ACK us");

        let mut buf = [0; 32];
        let res = conn.read(&mut buf).await?;
        let msg = rsmp::Rsmp::parse_bytes(&buf[..res])?;

        if !rsmp::is_ack(&msg) {
            return Err(HandleError::Sync("Replica did not ack subscription".into()));
        };

        let snapshot_hash = store::utils::hash_snapshot(&state);
        conn.write(&snapshot_hash[..]).await?;

        info!(target: "connections", "Sending store to subscriber: {uuid}");

        let total_bytes = state.len();
        let mut bytes_sent = 0;

        while total_bytes > 0 && bytes_sent < total_bytes {
            match conn.write(&state[bytes_sent..]).await? {
                0 => {
                    return Err(HandleError::Sync(
                        "Replica closed connection between state transfer".into(),
                    ))
                }
                b => bytes_sent += b,
            }
        }

        conn.write(rsmp::RAW_BYTES_END).await?;

        SyncedConnectionPacket::new(uuid, addr, conn, node, store, snapshot_ts)
            .dispatch()
            .await;

        info!(target: "connections", "Store sent successfully to {} [{}] ({}) bytes.",
        uuid, hex::encode(snapshot_hash), bytes_sent);
    }

    Ok(())
}

pub(crate) async fn redirect<Stream, Store>(
    args: &Args,
    mut conn: Connection<Stream>,
    node: NodeRef,
    store: SharedStore<Store>,
) -> Result<(), HandleError>
where
    Stream: AsyncStream,
    Store: Storage,
{
    attempt!(&mut conn, {
        let (addr,) = expect_args!(args, SocketAddr);

        let remote = Connection::from_addr(addr)
            .await
            .map_err(|e| HandleError::Sync(format!("Master refused to connect: {e:?}")))?;

        let master_uuid = sync_to_master(remote, node.clone(), store).await?;
        node.promote(net::InstanceRole::Replica(Mode::Child(master_uuid, addr)))
            .await;

        Ok::<(), HandleError>(())
    })
}

pub(crate) async fn confget<Stream, Store>(
    _args: &Args,
    mut conn: Connection<Stream>,
    node: NodeRef,
    _: SharedStore<Store>,
) -> Result<(), HandleError>
where
    Stream: AsyncStream,
    Store: Storage,
{
    // TODO: just get read lock once
    const DEFAULT_CONF_FIELDS: &[&[u8]] = &[b"UUID", b"ADDR", b"ROLE", b"REPLCOUNT", b"REPLINFO"];

    let conf = attempt!(&mut conn, {
        let mut out = String::new();
        for field in DEFAULT_CONF_FIELDS {
            match field.to_ascii_uppercase().as_slice() {
                b"UUID" => out.push_str(&format!("UUID={}\n", node.uuid().await)),
                b"ADDR" => out.push_str(&format!("ADDR={}\n", node.addr().await)),
                b"ROLE" => out.push_str(&format!("ROLE={}\n", node.role().await)),
                b"REPLCOUNT" => out.push_str(&format!(
                    "REPLCOUNT={}\n",
                    node.read().await.replcount().await
                )),
                b"REPLINFO" => {
                    let nlock = node.read().await;
                    let rlock = nlock.replicas().await;
                    let replica_data: Vec<(SocketAddr, Uuid)> =
                        rlock.iter().map(|r| (r.addy, r.uuid)).collect();
                    drop(rlock);
                    drop(nlock);
                    replica_data
                        .iter()
                        .for_each(|(a, u)| out.push_str(&format!("REPLICA={a}:{u}\n")));
                }
                _ => Err(HandleError::InvalidArgument(
                    String::from_utf8_lossy(field).to_string(),
                ))?,
            }
        }
        Ok::<_, HandleError>(out)
    })?;

    conn.write(&conf.into_bytes()).await?;

    Ok(())
}

async fn promote_inner<Stream: AsyncStream>(
    uuid: Uuid,
    mut conn: Connection<Stream>,
    node: NodeRef,
) -> Result<(), HandleError> {
    attempt!(&mut conn, {
        let rlock = node.read().await;
        let replicas = rlock.replicas().await;
        let target = replicas
            .iter()
            .find(|r| r.uuid == uuid)
            .ok_or(HandleError::Sync(format!(
                "Replica with uuid: {uuid} doesn't exist"
            )))?;
        let uuid = rlock.uuid();
        let port = rlock.addr().port();
        let promotion = Command::promote(args!(uuid.as_bytes(), &port.to_be_bytes()));
        target.send(promotion).await.map_err(|e| {
            HandleError::Sync(format!(
                "Failed to promote replica, could not pass message: {e}"
            ))
        })?;

        // wait for message of remote ACKing promo
        target.wait_promoted_signal().await.map_err(|e| {
            HandleError::Sync(format!(
                "Failed to promote replica, server responded with: {e}"
            ))
        })?;

        drop(replicas);
        rlock.replicas_mut().await.retain(|r| r.uuid != uuid);

        // NOTE: src/net/mod.rs::dispatch_sync_thread
        // catches SyncCommand::Promote, sends the current connection to a new
        // sync thread and exits the replication thread
        // That task also has the full context, uuid, address and listening port
        // of the replica that was promoted, so that task is in charge of updating
        // this instance's role to replica once the handshake is successfull and
        // handling the new synchronization between this instance and the new master
        Ok::<_, HandleError>(())
    })?;

    conn.write(&rsmp::OK.to_bytes()).await?;

    Ok(())
}

pub(crate) async fn repl_promote<Stream, Store>(
    args: &Args,
    conn: Connection<Stream>,
    node: NodeRef,
    _: SharedStore<Store>,
) -> Result<(), HandleError>
where
    Stream: AsyncStream,
    Store: Storage,
{
    let (uuid, _) = expect_args!(args, Uuid, &str);
    promote_inner(uuid, conn, node).await
}

pub(crate) async fn promote<Stream, Store>(
    args: &Args,
    conn: Connection<Stream>,
    node: NodeRef,
    _: SharedStore<Store>,
) -> Result<(), HandleError>
where
    Stream: AsyncStream,
    Store: Storage,
{
    let (uuid,) = expect_args!(args, Uuid);
    promote_inner(uuid, conn, node).await
}

pub(crate) async fn shutdown<Stream, Store>(
    args: &Args,
    mut conn: Connection<Stream>,
    node: NodeRef,
    _: SharedStore<Store>,
) -> Result<(), HandleError>
where
    Stream: AsyncStream,
    Store: Storage,
{
    // Args: [sleep_ms optional]
    // sleep n amount of millis so we give time for broadcasters to let replicas know we are shutting down
    node.shutdown().await.map_err(|_| {
        HandleError::InvalidOp(
            "Shutdown channel closed, this code should be unreachable, sorry :^(",
        )
    })?;

    node.broadcast(Command::shutdown(args!())).await;
    conn.write(&OK.to_bytes()).await?;

    let timeout_ms = args
        .deser_iter()
        .next::<usize>()
        .unwrap_or(crate::net::DEFAULT_SHUTDOWN_SLEEP_MS);

    sleep(Duration::from_millis(timeout_ms as u64)).await;

    Ok(())
}

pub(crate) async fn adopt<Stream, Store>(
    args: &Args,
    mut conn: Connection<Stream>,
    node: NodeRef,
    _: SharedStore<Store>,
) -> Result<(), HandleError>
where
    Stream: AsyncStream,
    Store: Storage,
{
    // ADOPT {addr}
    // send instance to pick up an orphan replica
    // instance sends revive to orphan replica

    attempt!(&mut conn, {
        // this should not go because if we have that uuid in replicas
        // it means it is not an orphan at all
        // let addr = {
        //     if let Ok(uuid) = args.deser_iter().next::<Uuid>() {
        //         node.read()
        //             .await
        //             .replicas()
        //             .await
        //             .iter()
        //             .find(|r| r.uuid == uuid)
        //             .map(|r| r.addy)
        //             .ok_or_else(|| {
        //                 HandleError::InvalidArgument(format!("Replica with uuid: {uuid} not found"))
        //             })?
        //     } else {
        //         expect_args!(args, SocketAddr).0
        //     }
        // };

        let mut remote = Connection::from_addr(expect_args!(args, SocketAddr).0)
            .await
            .map_err(|e| HandleError::Sync(format!("Remote refused to connect: {e}")))?;

        let addr = node.addr().await.to_string();

        remote
            .write(&Rsmp::from(&Command::revive(args!([addr.as_bytes()]))).to_bytes())
            .await?;

        let mut res = [0; 256];
        let b = remote.read(&mut res).await?;
        let res = rsmp::Rsmp::parse_bytes(&res[..b])?;
        if res != rsmp::OK {
            return Err(HandleError::Sync(format!(
                "Remote refused to adopt: {}",
                String::from_utf8_lossy(&res.to_bytes())
            )));
        };
        Ok::<_, HandleError>(())
    })?;

    conn.write(&rsmp::OK.to_bytes()).await?;
    Ok(())
}

pub(crate) async fn revive<Stream, Store>(
    args: &Args,
    mut conn: Connection<Stream>,
    node: NodeRef,
    store: SharedStore<Store>,
) -> Result<(), HandleError>
where
    Stream: AsyncStream,
    Store: Storage,
{
    if node.role().await != InstanceRole::Replica(Mode::Orphan) {
        attempt!(&mut conn, {
            Err(HandleError::InvalidOp(
                "REVIVE command should only be sent to orphaned replicas",
            ))
        })?
    };

    attempt!(&mut conn, {
        let (addr,) = expect_args!(args, SocketAddr);
        let remote = Connection::from_addr(addr).await?;
        sync_to_master(remote, node.clone(), store).await?;
        Ok::<_, HandleError>(())
    })?;

    conn.write(&rsmp::OK.to_bytes()).await?;
    Ok(())
}

impl From<store::Error> for Rsmp {
    fn from(err: store::Error) -> Rsmp {
        Rsmp::Error(Bytes::copy_from_slice(err.as_bytes()))
    }
}
