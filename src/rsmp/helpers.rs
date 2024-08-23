use bytes::Bytes;

pub const RAW_FULL_SYNC: &[u8] = b"$FULL+SYNC\r\n";
pub const RAW_PARTIAL_SYNC: &[u8] = b"$PARTIAL+SYNC\r\n";
pub const RAW_BYTES_END: &[u8] = b"$DBSTREAM+END\r\n";

pub static PONG: super::Rsmp = super::Rsmp::String(Bytes::from_static(b"PONG"));
pub static OK: super::Rsmp = super::Rsmp::String(Bytes::from_static(b"OK"));
pub static ACK: super::Rsmp = super::Rsmp::String(Bytes::from_static(b"ACK"));

pub fn is_ack(s: &super::Rsmp) -> bool {
    match s {
        super::Rsmp::String(_) => *s == ACK,
        super::Rsmp::Array(ref v) => matches!(&v[..], [s] if *s == ACK),
        _ => false,
    }
}
