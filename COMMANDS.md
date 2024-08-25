### MASTER
- #### Owned commands
    - [x] **PING**
    > Write a `$PONG\r\n` response into the connection.
    - [x] **WRITE** `query[*]`
    > Execute a write operation  with the implementor's defined querying logic and writes the results into the connection. Then the same command is propagated to each replica.
    - [x] **READ** `query[*]`
    > Execute a read operation with the implementor's defined querying logic, and writes the results into the connection.
    - [x] **REDIRECT** `host:port`
    > Connects to the utf-8 formatted `host:port` (target), attempts to get a subscription and downgrades self into replica
    > - instance sends `SUBSCRIBE uuid port` to target
    > - target sends `ACK`
    > - instance sends `ACK`
    > - target serializes storage and sends it in chunks
    > - instance reads and stores chunks until `$DBSTREAM+END\r\n`
    > - instance serializes bytes into store and saves it
    > - instance sends current connection into sync routine thread and executes mutations from target
    > - instance downgrades role to `REPLICA`
    - [x] **PROMOTE** `uuid`
    > Promotes a stored target to `MASTER` and downgrades self
    > - search instance.replicas for `replica.uuid == UUID` (target)
    > - send `PROMOTE {instance_uuid} {instance_listening_port}` command to target
    > - downgrades instance into `SLAVE`
    > - instance sends current connection into sync routine thread and executes mutations from target
    > - instance keeps replicating commands sent from target
    - [x] **SUBSCRIBE** `uuid` `port`
    > Sends current storage state to incoming connection, saves it and sends all further mutations
    > `When a replica server first starts, it attemps to subscribe to its designated parent instance, getting the full state of the storage an saving the current connection to process incoming mutations`. If a timestamp is present we attempt to execute a partial sync if possible.
    > - instance receives SUBSCRIBE `uuid` `port` `[timestamp]`
    > - instance parses uuid, port and serializes storage into bytes
    > - instance `ACK`s the subscriber
    > - subscriber `ACK`s instance
    > - instance sends the storage bytes in chunks
    > - instance sends `$DBSTREAM+END\r\n` bytes signaling end of transfer
    > - instance saves subscriber connection data into replica list
    - [x] **ADOPT** `host:port`
    > Connects to the utf-8 formatted `host:port` (target) and sends a `REVIVE {instance_addr}` where instance_addr is this instance's (source) listening addr
    > - target receives REVIVE `addr`
    > - target connects to addr (source) and executes a subscribe
    > - target sends `OK` letting source know that adoption went as expected
    - [x] **REVIVE**
    > Error, command should only be sent to orphaned replicas, master instance won't subscribe to anything
    - [x] **CONFGET** `[field1[ field2[ ...n]]`
    > Reads field values from instance's config, if no field provided reads whole config 
    - [x] **SHUTDOWN** `[timeout_ms]`
    > Exits the listening thread an returns from serve, it also broadcasts a shutdown command to each replica, to let them know that the source is exiting and mark themselves as orphaned, if timeout_ms is provided, the executing thread will wait that time to let the replicas send their messages, if no timeout provided, default is 5 seconds

<br>

___

### REPLICA
- #### Owned commands
    - [x] **WRITE** `query`
    > **NOT-ALLOWED**
    - [x] **PROMOTE** `[uuid]`
    > NOT-ALLOWED
    - [x] **REVIVE** `host:port`
    > If replica is orphaned it connects to the addr and executes the regular subscribe sync command, after it is complete, sends back `OK` to let source know adoption went as expected

- #### Same impl as `master.command`
    - [x] *PING*
    - [x] *CONFGET* `[field1[ field2[ ...n]]`
    - [x] *READ* `query`
    - [x] *REDIRECT* `host:port`
    - [x] *SUBSCRIBE* `uuid` `port`
    - [x] *ADOPT*
    - [x] *SHUTDOWN*


- ### Sync commands
    - [x] **PING**
    > **NOT-ALLOWED**
    - [x] **READ**
    > **NOT-ALLOWED**
    - [x] **SUBSCRIBE**
    > **NOT-ALLOWED**
    - [x] **ADOPT**
    > **NOT-ALLOWED**
    - [x] **REVIVE**
    > **NOT-ALLOWED**
    - [x] **CONFGET**
    > **NOT-ALLOWED**
    - [x] **WRITE**
    > Execute a write operation  with the implementor's defined querying logic without writing the results into the connection, then the same command is propagated to each replica.
    - [x] **REDIRECT** `host:port`
    > Connects to the utf-8 formatted `host:port` (target), attempts to get a subscription
    > - instance sends `SUBSCRIBE uuid port` to target
    > - target sends `ACK`
    > - instance sends `ACK`
    > - target serializes storage and sends it in chunks
    > - instance reads and stores chunks until `$DBSTREAM+END\r\n`
    > - instance serializes bytes into store and saves it
    > - instance sends current connection into sync routine thread and executes mutations from target
    - [x] PROMOTE `parent_uuid` `parent_port`
    > Promotes instance to `MASTER` saving the current connection into replica list and exiting the current sync thread
    > `Happens when a master instance previously got a `PROMOTE uuid` command`
    > - set instance role to `MASTER`
    > - saves `parent_uuid` `parent_port` and `connection` into replica list
    > - exit current sync thread
    - [x] **SHUTDOWN**
    > Exits the sync thread and marks itself as orphaned, this instance can then be `REVIVE'd` by another one, and resync happens