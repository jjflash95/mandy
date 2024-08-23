mod errors;

pub use self::errors::CommandError;
use crate::rsmp::Rsmp;

use bytes::Bytes;
use std::sync::Arc;

pub type Args = Arc<[Bytes]>;

/// Utility to create an RSMP formatted command
///
/// ```
/// use mandy::rsmp::Rsmp;
/// use mandy::command;
///
/// let write_command = command!["WRITE", "HELLO", "WORLD"];
///
/// assert_eq!(&write_command.to_bytes(), b"+3\r\n$WRITE\r\n$HELLO\r\n$WORLD\r\n");
/// ```
#[macro_export]
macro_rules! command {
    ($($x:expr),* $(,)?) => {
        {
            let mut temp_vec = Vec::new();
            $(
                temp_vec.push($x);
            )*
            Rsmp::from(temp_vec)
        }
    };
}

#[macro_export]
macro_rules! args {
    () => {
        Arc::new([])
    };
    ($x: expr) => {
        Arc::from($x.iter().map(|b| Bytes::copy_from_slice(b)).collect::<Vec<Bytes>>().into_boxed_slice())
    };
    ($($x:expr),+) => {
        Arc::from(vec![$(Bytes::copy_from_slice($x)),*].into_boxed_slice())
    }
}

#[macro_export]
macro_rules! expect_args {
    ($args:expr, $($t:ty),*) => {
        {
            let mut iter = $args.deser_iter();
            (
                $(
                    iter.next::<$t>()?,
                )*
            )
        }
    };
}

pub type Result<T> = std::result::Result<T, CommandError>;

#[derive(Clone)]
/// Cheaply clonnable Command, args are behind and Arc<[Bytes]>
pub struct Command {
    pub kind: CommandKind,
    pub args: Args,
}

#[derive(Clone, Debug)]
pub enum CommandKind {
    Shutdown,
    Ping,
    Subscribe,
    Promote,
    Redirect,
    Adopt,
    Revive,
    ConfGet,
    Write,
    Read,
}

impl Command {
    pub(crate) fn ping(args: Args) -> Self {
        Self {
            kind: CommandKind::Ping,
            args,
        }
    }

    pub(crate) fn subscribe(args: Args) -> Self {
        Self {
            kind: CommandKind::Subscribe,
            args,
        }
    }

    pub(crate) fn promote(args: Args) -> Self {
        Self {
            kind: CommandKind::Promote,
            args,
        }
    }

    pub(crate) fn redirect(args: Args) -> Self {
        Self {
            kind: CommandKind::Redirect,
            args,
        }
    }

    pub(crate) fn conf_get(args: Args) -> Self {
        Self {
            kind: CommandKind::ConfGet,
            args,
        }
    }

    pub(crate) fn write(args: Args) -> Self {
        Self {
            kind: CommandKind::Write,
            args,
        }
    }

    pub(crate) fn read(args: Args) -> Self {
        Self {
            kind: CommandKind::Read,
            args,
        }
    }

    pub(crate) fn shutdown(args: Args) -> Self {
        Self {
            kind: CommandKind::Shutdown,
            args,
        }
    }

    pub(crate) fn adopt(args: Args) -> Self {
        Self {
            kind: CommandKind::Adopt,
            args,
        }
    }

    pub(crate) fn revive(args: Args) -> Self {
        Self {
            kind: CommandKind::Revive,
            args,
        }
    }

    pub(crate) fn from_rsmp(rsmp: Rsmp) -> Result<Self> {
        match rsmp {
            Rsmp::String(s) => Self::from_bytes(s),
            Rsmp::Array(v) => Self::from_rsmp_vec(v),
            _ => Err(CommandError::InvalidCommand(rsmp.to_debug_string())),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn args(&self) -> &Args {
        &self.args
    }

    #[allow(dead_code)]
    pub(crate) fn into_args(self) -> Args {
        self.args
    }

    fn from_bytes(s: Bytes) -> Result<Self> {
        match s {
            _ if &s[..] == b"PING" => Ok(Self {
                kind: CommandKind::Ping,
                args: Arc::new([]),
            }),
            _ => Err(CommandError::InvalidCommand(format!(
                "Invalid command or missing parameters: {:?}",
                String::from_utf8_lossy(&s[..])
            ))),
        }
    }

    fn from_rsmp_vec(v: Vec<Rsmp>) -> Result<Self> {
        if v.is_empty() {
            return Err(CommandError::InvalidLength("Empty command".into()));
        }

        let Some(mut v) = v
            .iter()
            .map(|m| m.core_bytes())
            .collect::<Option<Vec<Bytes>>>()
        else {
            return Err(CommandError::InvalidCommand(format!("{:?}", v[0])));
        };

        let args = args!(v.split_off(1));
        let command = &v[0];
        match command {
            _ if &command[..] == b"PING" => Ok(Self::ping(args)),
            _ if &command[..] == b"SUSCRIBE" => Ok(Self::subscribe(args)),
            _ if &command[..] == b"SUBSCRIBE" => Ok(Self::subscribe(args)),
            _ if &command[..] == b"PROMOTE" => Ok(Self::promote(args)),
            _ if &command[..] == b"REDIRECT" => Ok(Self::redirect(args)),
            _ if &command[..] == b"ADOPT" => Ok(Self::adopt(args)),
            _ if &command[..] == b"CONFGET" => Ok(Self::conf_get(args)),
            _ if &command[..] == b"WRITE" => Ok(Self::write(args)),
            _ if &command[..] == b"READ" => Ok(Self::read(args)),
            _ if &command[..] == b"SHUTDOWN" => Ok(Self::shutdown(args)),
            _ if &command[..] == b"REVIVE" => Ok(Self::revive(args)),
            _ => {
                let e = v
                    .iter()
                    .map(|m| String::from_utf8_lossy(m).to_string())
                    .collect::<Vec<String>>();
                Err(CommandError::InvalidCommand(format!(
                    "Invalid command: {:?}",
                    e
                )))
            }
        }
    }
}

impl From<&Command> for Rsmp {
    fn from(command: &Command) -> Self {
        let kind: &[u8] = match command.kind {
            CommandKind::Ping => b"PING",
            CommandKind::Write => b"WRITE",
            CommandKind::Read => b"READ",
            CommandKind::ConfGet => b"CONFGET",
            CommandKind::Subscribe => b"SUBSCRIBE",
            CommandKind::Promote => b"PROMOTE",
            CommandKind::Adopt => b"ADOPT",
            CommandKind::Redirect => b"REDIRECT",
            CommandKind::Revive => b"REVIVE",
            CommandKind::Shutdown => b"SHUTDOWN",
        };

        let args = command
            .args
            .iter()
            .map(|s| Rsmp::String(s.clone()))
            .collect::<Vec<Rsmp>>();

        let mut rsmp = Vec::with_capacity(command.args.len() + 1);
        rsmp.push(Rsmp::String(Bytes::copy_from_slice(kind)));
        rsmp.extend(args);
        Rsmp::Array(rsmp)
    }
}

impl std::fmt::Debug for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        fn fmt_args(args: &Args) -> Vec<String> {
            args.iter()
                .map(|s| String::from_utf8_lossy(s).to_string())
                .collect()
        }

        match self.kind {
            CommandKind::Ping => write!(f, "PING {:?}", fmt_args(&self.args)),
            CommandKind::Subscribe => write!(f, "SUBSCRIBE {:?}", fmt_args(&self.args)),
            CommandKind::Promote => write!(f, "PROMOTE {:?}", fmt_args(&self.args)),
            CommandKind::Redirect => write!(f, "REDIRECT {:?}", fmt_args(&self.args)),
            CommandKind::Revive => write!(f, "REVIVE {:?}", fmt_args(&self.args)),
            CommandKind::Adopt => write!(f, "ADOPT {:?}", fmt_args(&self.args)),
            CommandKind::ConfGet => write!(f, "CONFGET {:?}", fmt_args(&self.args)),
            CommandKind::Write => write!(f, "WRITE {:?}", fmt_args(&self.args)),
            CommandKind::Read => write!(f, "READ {:?}", fmt_args(&self.args)),
            CommandKind::Shutdown => write!(f, "SHUTDOWN {:?}", fmt_args(&self.args)),
        }
    }
}

pub trait SafeRemove<T: std::fmt::Debug + Default, E> {
    fn safe_remove(&mut self, i: usize) -> std::result::Result<T, E>;
}

impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.kind {
            CommandKind::Ping => write!(f, "PING"),
            CommandKind::Shutdown => write!(f, "SHUTDOWN"),
            CommandKind::Subscribe => write!(f, "SUBSCRIBE"),
            CommandKind::Promote => write!(f, "PROMOTE"),
            CommandKind::Adopt => write!(f, "ADOPT"),
            CommandKind::Redirect => write!(f, "REDIRECT"),
            CommandKind::Revive => write!(f, "REVIVE"),
            CommandKind::ConfGet => write!(f, "CONFGET"),
            CommandKind::Write => write!(f, "WRITE"),
            CommandKind::Read => write!(f, "READ"),
        }
    }
}

impl<T: std::fmt::Debug + Default> SafeRemove<T, CommandError> for Vec<T> {
    fn safe_remove(&mut self, i: usize) -> Result<T> {
        if let Some(t) = self.get_mut(i) {
            Ok(std::mem::take(t))
        } else {
            Err(CommandError::InvalidLength(format!(
                "Missing parameter {} on {:?}",
                i, self,
            )))
        }
    }
}
