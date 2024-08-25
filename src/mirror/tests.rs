#[cfg(test)]
#[allow(clippy::unused_io_amount, clippy::module_inception)]
mod tests {
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;
    use tokio::time::sleep;
    use uuid::Uuid;

    use crate::mirror::{actions, Config, Role};
    use crate::net::{tests::*, Connection};
    use crate::store::{self, Readable, Sendable, Writable};
    use crate::mirror::Mirror;
    use crate::vec_from;
    use std::fmt::Debug;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    type SharedBuffer = Arc<Mutex<Vec<Message>>>;

    use tokio::sync::OnceCell;

    static LOGGER: OnceCell<()> = OnceCell::const_new();

    async fn setup_logger() {
        LOGGER
            .get_or_init(|| async {
                simple_logger::init_with_level(log::Level::Trace).unwrap();
            })
            .await;
    }

    #[tokio::test]
    async fn write() {
        setup_logger().await;

        let config = Config::new(None, "127.0.0.1:6111".parse().unwrap(), Role::Master);
        let mirror = Mirror::init(config, Store::default()).await.unwrap();

        let inner = slice_to_rm(vec![b"+2\r\n$WRITE\r\n$1\r\n"]);
        let outer = slice_to_rm(vec![]);
        let conn = make_conn(inner.clone(), outer.clone());

        mirror.handle(conn);
        sleep(Duration::from_nanos(1)).await;

        let slice = { &mirror.store.read().await.inner.clone() };
        assert_eq!(slice, &[1]);

        let inner = slice_to_rm(vec![b"+2\r\n$WRITE\r\n$2\r\n"]);
        let outer = slice_to_rm(vec![]);
        let conn = make_conn(inner.clone(), outer.clone());

        mirror.handle(conn);

        sleep(Duration::from_nanos(1)).await;
        let slice = { &mirror.store.read().await.inner.clone() };
        assert_eq!(slice, &[1, 2]);
    }

    #[tokio::test]
    async fn master_write_ok() {
        setup_logger().await;

        let config = Config::new(None, "127.0.0.1:6111".parse().unwrap(), Role::Master);
        let mirror = Mirror::init(config, Store::default()).await.unwrap();
        let inner = slice_to_rm(vec![b"+2\r\n$WRITE\r\n$1\r\n"]);
        let outer = slice_to_rm(vec![]);
        let conn = make_conn(inner.clone(), outer.clone());

        mirror.handle(conn);
        sleep(Duration::from_millis(1)).await;
        assert_eq!(std::str::from_utf8(&outer.r(0)), Ok("1"));
    }

    #[tokio::test]
    async fn replica_write_fails() {
        setup_logger().await;

        let master = Mirror::init(
            Config::new(None, "127.0.0.1:6111".parse().unwrap(), Role::Master),
            Store::default(),
        )
        .await
        .unwrap();

        let t = tokio::spawn(async move { master.serve().await });

        sleep(Duration::from_millis(1)).await;

        let config = Config::new(None, "127.0.0.1:6111".parse().unwrap(), Role::Replica("127.0.0.1:6111".parse().unwrap()));
        let mirror = Mirror::init(config, Store::default()).await.unwrap();
        let inner = slice_to_rm(vec![b"+2\r\n$WRITE\r\n$1\r\n"]);
        let outer = slice_to_rm(vec![]);
        let conn = make_conn(inner.clone(), outer.clone());

        mirror.handle(conn);
        sleep(Duration::from_millis(1)).await;
        assert_eq!(outer.r(0), b"!Can't perform WRITE on replica\r\n",);
        t.abort();
        sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn subscribe_fullsync() {
        setup_logger().await;

        let config = Config::new(None, "127.0.0.1:6111".parse().unwrap(), Role::Master);
        let mirror = Mirror::init(config, Store::default()).await.unwrap();
        {
            let messages: Vec<&[u8]> = vec![b"+2\r\n$WRITE\r\n$4\r\n"];
            let inner = slice_to_rm(messages);
            let outer = slice_to_rm(vec![]);
            let conn = make_conn(inner.clone(), outer.clone());
            mirror.handle(conn);

            let messages: Vec<&[u8]> = vec![b"+2\r\n$WRITE\r\n$2\r\n"];
            let inner = slice_to_rm(messages);
            let outer = slice_to_rm(vec![]);
            let conn = make_conn(inner.clone(), outer.clone());
            mirror.handle(conn);

            let messages: Vec<&[u8]> = vec![b"+2\r\n$WRITE\r\n$0\r\n"];
            let inner = slice_to_rm(messages);
            let outer = slice_to_rm(vec![]);
            let conn = make_conn(inner.clone(), outer.clone());
            mirror.handle(conn);
        }
        let messages: Vec<&[u8]> = vec![
            b"+3\r\n$SUBSCRIBE\r\n$\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\r\n$6123\r\n",
            b"+1\r\n$ACK\r\n",
        ];
        let inner = slice_to_rm(messages);
        let outer = slice_to_rm(vec![]);
        let conn = make_conn(inner.clone(), outer.clone());

        mirror.handle(conn);
        sleep(Duration::from_nanos(1)).await;

        assert_eq!(outer.r(0), vec_from!(&config.uuid.into_bytes(), crate::rsmp::RAW_FULL_SYNC));
        assert_eq!(outer.r(1), store::utils::hash_snapshot(&[4, 2, 0]));
        assert_eq!(outer.r(2), &[4, 2, 0]);
        assert_eq!(outer.r(3), b"$DBSTREAM+END\r\n");
    }

    #[tokio::test]
    async fn subscribe_fullsync_big_store() {
        setup_logger().await;

        let one_gb = 1024 * 1024 * 1024;
        let big_store = Store {
            inner: vec![0_u8; one_gb],
        };
        let config = Config::new(None, "127.0.0.1:6111".parse().unwrap(), Role::Master);
        let mirror = Mirror::init(config, big_store).await.unwrap();
        {
            let messages: Vec<&[u8]> = vec![b"+2\r\n$WRITE\r\n$4\r\n"];
            let inner = slice_to_rm(messages);
            let outer = slice_to_rm(vec![]);
            let conn = make_conn(inner.clone(), outer.clone());
            mirror.handle(conn);

            let messages: Vec<&[u8]> = vec![b"+2\r\n$WRITE\r\n$2\r\n"];
            let inner = slice_to_rm(messages);
            let outer = slice_to_rm(vec![]);
            let conn = make_conn(inner.clone(), outer.clone());
            mirror.handle(conn);

            let messages: Vec<&[u8]> = vec![b"+2\r\n$WRITE\r\n$0\r\n"];
            let inner = slice_to_rm(messages);
            let outer = slice_to_rm(vec![]);
            let conn = make_conn(inner.clone(), outer.clone());
            mirror.handle(conn);
        }

        let messages: Vec<&[u8]> = vec![
            b"+3\r\n$SUBSCRIBE\r\n$\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\r\n$6123\r\n",
            b"+1\r\n$ACK\r\n",
        ];

        let inner = slice_to_rm(messages);
        let outer = slice_to_rm(vec![]);
        let conn = make_conn(inner.clone(), outer.clone());

        mirror.handle(conn);

        sleep(Duration::from_nanos(1)).await;

        let expected = vec_from!(&vec![0_u8; one_gb], &[4, 2, 0]);

        assert_eq!(outer.r(0), vec_from!(&config.uuid.into_bytes(), crate::rsmp::RAW_FULL_SYNC));
        assert_eq!(outer.r(1), store::utils::hash_snapshot(&expected));
        assert_eq!(outer.r(2), expected);
        assert_eq!(outer.r(3), b"$DBSTREAM+END\r\n");
    }

    #[tokio::test]
    async fn adopt_orphaned() {
        // - spin up master instance
        // - spin up replica
        // - shutdown master instance
        // - spin up new master
        // - send adopt command to new master
        // - replica should answer with OK
        setup_logger().await;

        let master_addr: SocketAddr = "127.0.0.1:6991".parse().unwrap();
        let replica_addr: SocketAddr = "127.0.0.1:6992".parse().unwrap();
        let new_master_addr:SocketAddr = "127.0.0.1:6993".parse().unwrap();

        let config = Config::new(
            None,
            master_addr,
            Role::Master,
        );
        let master = Mirror::init(config, Store::default()).await.unwrap();
        let handle = tokio::spawn(async move {
            master.serve().await.unwrap();
        });

        sleep(Duration::from_millis(5)).await;
        let config = Config::new(
            None,
            replica_addr,
            Role::Replica(master_addr),
        );
        let replica = Mirror::init(config, Store::default()).await.unwrap();
        tokio::spawn(async move {
            replica.serve().await.unwrap();
        });

        let mut c = Connection::from_addr(master_addr).await.unwrap();
        c.write(b"+1\r\n$SHUTDOWN\r\n").await.unwrap();
        sleep(Duration::from_millis(5)).await;

        let mut res = [0; 1024];
        let b = c.read(&mut res).await.unwrap();

        assert_eq!(&res[..b], b"$OK\r\n");
        handle.await.unwrap();

        let config = Config::new(
            None,
            new_master_addr,
            Role::Master,
        );
        let new_master = Mirror::init(config, Store::default()).await.unwrap();
        tokio::spawn(async move {
            new_master.serve().await.unwrap();
        });

        sleep(Duration::from_millis(5)).await;

        let mut new_master_conn = Connection::from_addr(new_master_addr).await.unwrap();
        new_master_conn.write(b"+2\r\n$ADOPT\r\n$127.0.0.1:6992\r\n").await.unwrap();
        let b = new_master_conn.read(&mut res).await.unwrap();
        assert_eq!(&res[..b], b"$OK\r\n");
    }


    #[tokio::test]
    async fn adopt_orphaned_fails() {
        // - spin up master instance
        // - spin up replica
        // - don't shutdown master instance
        // - spin up new master
        // - send adopt command to new master
        // - replica should answer with error
        setup_logger().await;

        let master_addr: SocketAddr = "127.0.0.1:6994".parse().unwrap();
        let replica_addr: SocketAddr = "127.0.0.1:6995".parse().unwrap();
        let new_master_addr:SocketAddr = "127.0.0.1:6996".parse().unwrap();

        let config = Config::new(
            None,
            master_addr,
            Role::Master,
        );
        let master = Mirror::init(config, Store::default()).await.unwrap();
        tokio::spawn(async move {
            master.serve().await.unwrap();
        });

        sleep(Duration::from_millis(5)).await;
        let config = Config::new(
            None,
            replica_addr,
            Role::Replica(master_addr),
        );
        let replica = Mirror::init(config, Store::default()).await.unwrap();
        tokio::spawn(async move {
            replica.serve().await.unwrap();
        });

        let config = Config::new(
            None,
            new_master_addr,
            Role::Master,
        );
        let new_master = Mirror::init(config, Store::default()).await.unwrap();
        tokio::spawn(async move {
            new_master.serve().await.unwrap();
        });

        sleep(Duration::from_millis(5)).await;

        let mut res = [0; 1024];
        let mut new_master_conn = Connection::from_addr(new_master_addr).await.unwrap();
        new_master_conn.write(b"+2\r\n$ADOPT\r\n$127.0.0.1:6992\r\n").await.unwrap();
        let b = new_master_conn.read(&mut res).await.unwrap();
        dbg!(String::from_utf8_lossy(&res[..b]));
        assert_eq!(&res[..b], b"!Remote refused to adopt: !REVIVE command should only be sent to orphaned replicas\r\n\r\n");
    }

    #[tokio::test]
    async fn subscribe_partialsync() {
        // Send message1
        // then subscribe with laggy connection to replica, asking for timestamp after message1
        // send message2 and message3 in a quick succession
        // by the time that subscribe ends (due to lag), message2 and message3 are already processed
        // assert that the laggy connection receives the deferred message2 and message3 even with bad
        // connection
        //
        // WRITE MESSAGE 1
        // WAIT 1 MS
        // SUBSCRIBE uuid port NOW_MS() -- starts
        // WRITE MESSAGE 2
        // WRITE MESSAGE 3
        // SUBSCRIBE uuid port NOW_MS() -- ends
        // DEFERRED MASTER SENDS MESSAGE 2
        // DEFERRED MASTER SENDS MESSAGE 3
        // ASSERT THAT WE GOT THOSE DEFERRED MESSAGES
        // ASSERT WE DON'T HAVE THE FULL STORE, JUST THE MESSAGES WE EXPECT
        // ![message2, message3].contains(message1)

        setup_logger().await;

        let message1 = b"+2\r\n$WRITE\r\n$4\r\n";
        let message2 = b"+2\r\n$WRITE\r\n$2\r\n";
        let message3 = b"+2\r\n$WRITE\r\n$0\r\n";

        let config = Config::new(None, "127.0.0.1:6111".parse().unwrap(), Role::Master);
        let mirror = Mirror::init(config, Store::default()).await.unwrap();

        {
            let messages: Vec<&[u8]> = vec![message1];
            let inner = slice_to_rm(messages);
            let outer = slice_to_rm(vec![]);
            mirror.handle(make_conn(inner, outer));
            sleep(Duration::from_millis(1)).await;
        }

        let mut sub =
            b"+4\r\n$SUBSCRIBE\r\n$\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\r\n$6123\r\n".to_vec();
        let ts = format!("${}", crate::net::ts_millis());
        sub.extend_from_slice(ts.as_bytes());
        sub.extend_from_slice(b"\r\n");

        let messages: Vec<&[u8]> = vec![sub.as_slice(), b"+1\r\n$ACK\r\n"];
        let inner = slice_to_rm(messages);
        let outer = slice_to_rm(vec![]);
        let laggy_sub = make_laggy_conn(inner.clone(), outer.clone(), Some(2));

        mirror.handle(laggy_sub);

        {
            let messages: Vec<&[u8]> = vec![message2];
            let inner = slice_to_rm(messages);
            let outer = slice_to_rm(vec![]);
            mirror.handle(make_conn(inner, outer));

            let messages: Vec<&[u8]> = vec![message3];
            let inner = slice_to_rm(messages);
            let outer = slice_to_rm(vec![]);
            mirror.handle(make_conn(inner, outer));
        }

        // allow laggy connection to receive updates
        sleep(Duration::from_millis(20)).await;

        dbg!("got", String::from_utf8_lossy(&outer.r(0)));
        dbg!("expected", String::from_utf8_lossy(&vec_from!(&config.uuid.into_bytes(), crate::rsmp::RAW_PARTIAL_SYNC)));
        assert_eq!(outer.r(0), vec_from!(&config.uuid.into_bytes(), crate::rsmp::RAW_PARTIAL_SYNC));
        assert_eq!(outer.r(1), message2);
        assert_eq!(outer.r(2), message3);
        assert_eq!(outer.lock().unwrap().iter().find(|m| *m == message1), None);
    }

    #[tokio::test]
    async fn replica_handshake() {
        setup_logger().await;

        let master_uuid = Uuid::new_v4();
        let shake = vec_from!(&master_uuid.into_bytes(), crate::rsmp::RAW_FULL_SYNC);

        let master_res: Vec<&[u8]> = vec![
            &shake,
            &[0; 32],
            &[6, 9, 4, 2, 0],
            b"$DBSTREAM+END\r\n",
        ];

        let inner = slice_to_rm(master_res);
        let outer = slice_to_rm(vec![]);
        let mut conn = make_conn(inner.clone(), outer.clone());

        let (recv_uuid, db) =
            actions::do_handshake(Uuid::nil(), "127.0.0.1:1".parse().unwrap(), &mut conn)
                .await
                .unwrap();

        assert_eq!(master_uuid, recv_uuid);
        assert_eq!(
            db,
            Some((
                store::utils::Hash(vec![0; 32]),
                store::utils::State(vec![6, 9, 4, 2, 0])
            ))
        )
    }

    #[tokio::test]
    async fn subscribe_fails_bad_uuid() {
        setup_logger().await;

        let config = Config::new(None, "127.0.0.1:6111".parse().unwrap(), Role::Master);
        let mirror = Mirror::init(config, Store::default()).await.unwrap();
        let messages: Vec<&[u8]> =
            vec![b"+3\r\n$SUBSCRIBE\r\n$\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\r\n$6123\r\n"];
        let inner = slice_to_rm(messages);
        let outer = slice_to_rm(vec![]);
        let conn = make_conn(inner.clone(), outer.clone());

        mirror.handle(conn);
        sleep(Duration::from_nanos(100)).await;
        assert_eq!(
            outer.r(0),
            b"!Malformed command parameter uuid::Uuid at index: 0\r\n"
        );
    }

    #[tokio::test]
    async fn subscribe_fails_bad_port() {
        setup_logger().await;

        let config = Config::new(None, "127.0.0.1:6111".parse().unwrap(), Role::Master);
        let mirror = Mirror::init(config, Store::default()).await.unwrap();
        let messages: Vec<&[u8]> =
            vec![b"+3\r\n$SUBSCRIBE\r\n$\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\r\n$612345\r\n"];
        let inner = slice_to_rm(messages);
        let outer = slice_to_rm(vec![]);
        let conn = make_conn(inner.clone(), outer.clone());

        mirror.handle(conn);
        sleep(Duration::from_nanos(100)).await;
        assert_eq!(
            outer.r(0),
            b"!Malformed command parameter u16 at index: 1\r\n"
        );
    }

    #[tokio::test]
    async fn replication_ok() {
        setup_logger().await;

        let config = Config::new(None, "127.0.0.1:6111".parse().unwrap(), Role::Master);
        let mirror = Mirror::init(config, Store::default()).await.unwrap();
        let messages: Vec<&[u8]> = vec![
            b"+3\r\n$SUBSCRIBE\r\n$\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\r\n$6123\r\n",
            b"$ACK\r\n",
        ];
        let replica_inner = slice_to_rm(messages);
        let replica_outer = slice_to_rm(vec![]);
        let conn = make_conn(replica_inner.clone(), replica_outer.clone());

        mirror.handle(conn);

        sleep(Duration::from_nanos(100)).await;

        let global_cmd = b"+2\r\n$WRITE\r\n$1\r\n";
        let commands: Vec<&[u8]> = vec![global_cmd];
        let cmd_inner = slice_to_rm(commands);
        let cmd_outer = slice_to_rm(vec![]);
        let cmd_conn = make_conn(cmd_inner.clone(), cmd_outer.clone());

        mirror.handle(cmd_conn);

        let global_cmd_2 = b"+2\r\n$WRITE\r\n$2\r\n";
        let commands: Vec<&[u8]> = vec![global_cmd_2];
        let cmd_inner = slice_to_rm(commands);
        let cmd_outer = slice_to_rm(vec![]);
        let cmd_conn = make_conn(cmd_inner.clone(), cmd_outer.clone());

        mirror.handle(cmd_conn);

        // important: since dispatch_sync_thread loop updates the cmd buffer every
        // 10 millis, we need to wait at least that time before sync commands start
        // getting dispatched
        sleep(Duration::from_millis(50)).await;

        assert_eq!(
            replica_outer.r(0),
            vec_from!(&config.uuid.into_bytes(), crate::rsmp::RAW_FULL_SYNC)
        );
        assert_eq!(replica_outer.r(1), store::utils::hash_snapshot(&[]));
        assert_eq!(replica_outer.r(2), b"$DBSTREAM+END\r\n");
        assert_eq!(replica_outer.r(3), global_cmd);
        assert_eq!(replica_outer.r(4), global_cmd_2);
    }

    #[tokio::test]
    async fn shutdown() {
        setup_logger().await;

        let addr = "127.0.0.1:6110".parse().unwrap();
        let config = Config::new(None, addr, Role::Master);
        let mirror = Mirror::init(config, Store::default()).await.unwrap();

        tokio::task::spawn(async move {
            let tcp = TcpStream::connect(addr).await.unwrap();
            let mut conn = Connection::new(tcp);
            conn.write(b"+1\r\n$SHUTDOWN\r\n").await.unwrap();
        });

        let r = tokio::select! {
            _ = mirror.serve() => Ok(()),
            _ = sleep(Duration::from_millis(5)) => Err("Server did not shutdown"),
        };

        assert_eq!(r, Ok(()))
    }

    #[tokio::test]
    async fn redirect_fails() {
        setup_logger().await;

        let config = Config::new(None, "127.0.0.1:6110".parse().unwrap(), Role::Master);
        let mirror = Mirror::init(config, Store::default()).await.unwrap();

        let messages: Vec<&[u8]> = vec![b"+2\r\n$REDIRECT\r\n$127.0.0.1:6114\r\n"];

        let inner = slice_to_rm(messages);
        let outer = slice_to_rm(vec![]);
        let conn = make_conn(inner.clone(), outer.clone());

        mirror.handle(conn);

        sleep(Duration::from_millis(10)).await;
        assert_eq!(
            outer.r(0),
            b"!Master refused to connect: Os { code: 61, kind: ConnectionRefused, message: \"Connection refused\" }\r\n"
        );
    }

    #[tokio::test]
    async fn redirect_ok() {
        setup_logger().await;

        let config = Config::new(None, "127.0.0.1:6789".parse().unwrap(), Role::Master);
        let master_store = Store {
            inner: vec![1, 2, 3],
        };

        let master_mirror = Mirror::init(config, master_store).await.unwrap();

        let t = tokio::spawn(async move {
            let _ = master_mirror.serve().await;
        });

        sleep(Duration::from_millis(1000)).await;

        let config = Config::new(None, "127.0.0.1:6111".parse().unwrap(), Role::Master);
        let mirror = Mirror::init(config, Store::default()).await.unwrap();
        let messages: Vec<&[u8]> = vec![b"+2\r\n$REDIRECT\r\n$127.0.0.1:6789\r\n"];

        let inner = slice_to_rm(messages);
        let outer = slice_to_rm(vec![]);
        let conn = make_conn(inner.clone(), outer.clone());

        mirror.handle(conn);

        sleep(Duration::from_millis(2000)).await;

        let addr: SocketAddr = "127.0.0.1:6789".parse().unwrap();
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write(b"+2\r\n$WRITE\r\n$4\r\n").await.unwrap();

        sleep(Duration::from_millis(2000)).await;
        assert_eq!(
            &[1, 2, 3, 4],
            mirror.store.read().await.inner.as_slice()
        );

        t.abort();
        sleep(Duration::from_millis(100)).await;
    }

    #[derive(Debug, Default)]
    struct Store {
        pub inner: Vec<u8>,
    }

    impl Writable for Store {
        type Input = u8;
        type Output = u8;

        fn write(&mut self, query: Vec<Self::Input>) -> Result<Self::Output, store::Error> {
            for n in &query {
                self.inner.push(*n)
            }

            Ok(query[0])
        }
    }

    impl Readable for Store {
        type Input = u8;
        type Output = u8;

        fn read(&self, query: Vec<Self::Input>) -> Result<Self::Output, store::Error> {
            Ok(query[0])
        }
    }

    impl Sendable for Store {
        fn serialize(&self) -> Result<Vec<u8>, store::Error> {
            dbg!(self.inner.clone());
            Ok(self.inner.clone())
        }

        fn initialize(&mut self, slice: Vec<u8>) -> Result<(), store::Error> {
            self.inner = slice;
            Ok(())
        }
    }

    pub trait WR {
        fn w(&self, idx: usize, b: &[u8]);
        fn r(&self, idx: usize) -> Vec<u8>;
    }

    impl WR for SharedBuffer {
        fn w(&self, idx: usize, b: &[u8]) {
            self.lock().unwrap()[idx] = b.to_vec();
        }

        fn r(&self, idx: usize) -> Vec<u8> {
            let inner = self.lock().unwrap();
            if idx + 1 > inner.len() {
                return vec![];
            }

            inner[idx].to_vec()
        }
    }

    fn make_conn(
        inner: Arc<Mutex<Vec<Vec<u8>>>>,
        outer: Arc<Mutex<Vec<Vec<u8>>>>,
    ) -> Connection<MockStream> {
        Connection::new(MockStream {
            inner_index: 0,
            outer_index: 0,
            inner,
            outer,
        })
    }
    fn make_laggy_conn(
        inner: Arc<Mutex<Vec<Vec<u8>>>>,
        outer: Arc<Mutex<Vec<Vec<u8>>>>,
        delay: Option<u64>,
    ) -> Connection<LaggyMockStream> {
        Connection::new(LaggyMockStream {
            ready: false,
            delay: delay.unwrap_or(10),
            inner_index: 0,
            outer_index: 0,
            inner,
            outer,
        })
    }

    fn slice_to_rm(slice: Vec<&[u8]>) -> Arc<Mutex<Vec<Vec<u8>>>> {
        Arc::new(Mutex::new(slice.into_iter().map(|v| v.to_vec()).collect()))
    }
}
