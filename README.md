# Mandy

A Rust library for creating custom data stores in a `master|replica` architecture, that is almost, but not quite, entirely unlike redis.

Replicas support both full and partial initialization from their parent instances, more on [supported commands](COMMANDS.md).

## Example

This example uses de `json` feature to leverage ser/deser operations to serde, and just focus on the logic, we implement `Readable` and `Writable` for our storage type, there is a third trait called `Sendable` which has a blanket implementation for types implementing `Serialize` and `Deserialize`

```rust
// src/pokedex.rs

use mandy::store::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub struct Pokemon {
    name: String,
    kind: PokemonKind,
    level: usize,
}

#[derive(Default, Serialize, Deserialize)]
pub struct Pokedex(pub std::collections::HashSet<Pokemon>);

impl Writable for Pokedex {
    type Input = Pokemon;
    type Output = ();

    fn write(&mut self, query: Vec<Self::Input>) -> Result<Self::Output, Error> {
        self.0.extend(query.into_iter());

        Ok(())
    }
}

impl Readable for Pokedex {
    type Input = Search;
    type Output = Vec<Pokemon>;

    fn read(&self, queries: Vec<Self::Input>) -> Result<Self::Output, Error> {
        let search = queries.first().cloned().unwrap_or_default();

        let res = self
            .0
            .iter()
            .filter(|p| match &search {
                Search::ALL => true,
                Search::NAME(ref name) => p.name == *name,
                Search::KIND(kind) => p.kind == *kind,
                Search::LEVEL(level) => p.level == *level,
            })
            .cloned()
            .collect();

        Ok(res)
    }
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub enum Search {
    #[default]
    ALL,
    NAME(String),
    KIND(PokemonKind),
    LEVEL(usize),
}

#[derive(Clone, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub enum PokemonKind {
    Fire,
    Grass,
    Water,
}
```

Creating an instance and handling incoming connections:

```rust
// src/main.rs

mod pokedex;

use tokio;
use log;
use mandy::{self, Config, Role};
use simple_logger;

#[tokio::main]
async fn main() {
    simple_logger::init_with_level(log::Level::Trace).unwrap();

    let config = Config::new(None, "127.0.0.1:6111".parse().unwrap(), Role::Master);
    let mirror = mandy::init(config, pokedex::Pokedex::default()).await.unwrap();

    mirror.serve().await.unwrap();
}
```

We can then `READ` and `WRITE` to our instance:
```rust
use mandy::command;
use mandy::rsmp::Rsmp;
use std::io::{self, Read, Write};
use std::net::SocketAddr;

fn main() -> Result<(), io::Error> {
    let mut buf = [0; 1024];
    let remote: SocketAddr = "127.0.0.1:6111".parse().unwrap();

    let cmd = command![
        "WRITE",
        r#"{"name": "Charizard", "level": 1, "kind": "Fire"}"#,
        r#"{"name": "Bulbasaur", "level": 99, "kind": "Grass"}"#,
        r#"{"name": "Squirtle", "level": 2, "kind": "Water"}"#,
        r#"{"name": "Gyarados", "level": 20, "kind": "Water"}"#,
    ];

    let mut conn = std::net::TcpStream::connect(remote)?;
    conn.write(&cmd.to_bytes())?;
    conn.read(&mut buf)?;

    println!("Write response is: {}", String::from_utf8_lossy(&buf));
    // Should output:
    // Write response is: null

    let cmd = command!["READ", r#"{"KIND": "Water"}"#];
    let mut conn = std::net::TcpStream::connect(remote)?;
    conn.write(&cmd.to_bytes())?;
    conn.read(&mut buf)?;

    println!("{}", String::from_utf8_lossy(&buf));
    // Should output:
    // [{"name":"Gyarados","kind":"Water","level":20},{"name":"Squirtle","kind":"Water","level":2}]

    Ok(())
}
```


## License

[MIT](https://choosealicense.com/licenses/mit/)