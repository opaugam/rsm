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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub blob: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct REPLICATE {
    pub id: u8,
    pub term: u64,
    pub check: u64,
    pub commit: u16,
    pub append: Vec<LogEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CONFIRM {
    pub id: u8,
    pub term: u64,
    pub ack: u16,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CONFLICT {
    pub id: u8,
    pub term: u64,
    pub try: u16,
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
    pub tail: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ADVERTISE {
    pub id: u8,
    pub term: u64,
    pub tail: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VOTE {
    pub id: u8,
    pub term: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct APPEND {
    pub id: u8,
    pub term: u64,
    pub blob: String,
}

declare!(1, REPLICATE);
declare!(2, CONFIRM);
declare!(3, CONFLICT);
declare!(4, UPGRADE);
declare!(5, CHECK);
declare!(6, ADVERTISE);
declare!(7, VOTE);
declare!(8, APPEND);

pub enum Command {
    MESSAGE(RAW),
    STORE(String),
    TIMEOUT(u64),
    HEARTBEAT,
}
