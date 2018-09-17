use bincode::serialize;

macro_rules! declare {
    ($code:expr, $msg:ident) => {
        impl $msg {
            pub(super) const CODE: u8 = $code;
            pub(super) fn to_raw(&self, src: &str, dst: &str) -> Vec<u8> {
                let raw = RAW {
                    code: $msg::CODE,
                    src: src.into(),
                    dst: dst.into(),
                    msg: serialize(&self).unwrap(),
                };
                serialize(&raw).unwrap()
            }
        }
    };
}

declare!(0, PING);
declare!(1, REPLICATE);
declare!(2, ACK);
declare!(3, REBASE);
declare!(4, UPGRADE);
declare!(5, PROBE);
declare!(6, AVAILABLE);
declare!(7, ADVERTISE);
declare!(8, VOTE);

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct RAW {
    pub(super) code: u8,
    pub(super) src: String,
    pub(super) dst: String,
    pub(super) msg: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct PING {
    pub(super) id: u8,
    pub(super) term: u64,
    pub(super) commit: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct REPLICATE {
    pub(super) id: u8,
    pub(super) term: u64,
    pub(super) off: u64,
    pub(super) age: u64,
    pub(super) commit: u64,
    pub(super) append: Vec<u8>,
    pub(super) snapshot: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ACK {
    pub(super) id: u8,
    pub(super) term: u64,
    pub(super) ack: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct REBASE {
    pub(super) id: u8,
    pub(super) term: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct UPGRADE {
    pub(super) id: u8,
    pub(super) term: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct PROBE {
    pub(super) id: u8,
    pub(super) term: u64,
    pub(super) head: u64,
    pub(super) age: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct AVAILABLE {
    pub(super) id: u8,
    pub(super) term: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ADVERTISE {
    pub(super) id: u8,
    pub(super) term: u64,
    pub(super) head: u64,
    pub(super) age: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct VOTE {
    pub(super) id: u8,
    pub(super) term: u64,
}
