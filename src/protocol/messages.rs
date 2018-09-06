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

/// Heartbeat message used to advertise the local state.
#[derive(Debug, Serialize, Deserialize)]
pub struct PING {
    pub id: u8,
    pub term: u32,
}

declare!(1, PING);