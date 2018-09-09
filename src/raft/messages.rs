use serde_json;
use serde_json::Value;
use std::fmt;

macro_rules! declare {
    ($code:expr, $msg:ident) => {
        impl $msg {
            pub const CODE: u8 = $code;
            pub fn to_raw(&self, src: &str, dst: &str) -> RAW {
                RAW {
                    code: $msg::CODE,
                    src: src.into(),
                    dst: dst.into(),
                    user: serde_json::to_value(&self).unwrap(),
                }
            }
        }
    };
}

/// Outer wrapper holding the source & destination hosts as well
/// as the actual message payload plus its identifier. Once serialized
/// the payload is a nested JSON object.
#[derive(Debug, Serialize, Deserialize)]
pub struct RAW {
    pub code: u8,
    pub src: String,
    pub dst: String,
    pub user: Value,
}

impl fmt::Display for RAW {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", serde_json::to_string(&self).unwrap())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PING {
    pub id: u8,
    pub term: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UPGRADE {
    pub id: u8,
    pub term: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CHECK {
    pub id: u8,
    pub term: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ADVERTISE {
    pub id: u8,
    pub term: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VOTE {
    pub id: u8,
    pub term: u64,
}

declare!(1, PING);
declare!(2, UPGRADE);
declare!(3, CHECK);
declare!(4, ADVERTISE);
declare!(5, VOTE);

pub enum Command {
    MESSAGE(RAW),
    TIMEOUT(u64),
    HEARTBEAT,
}
