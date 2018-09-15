use bincode::serialize;

macro_rules! declare {
    ($code:expr, $msg:ident) => {
        impl $msg {
            pub const CODE: u8 = $code;
            pub fn to_raw(&self, src: &str, dst: &str) -> Vec<u8> {
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
pub struct RAW {
    pub code: u8,
    pub src: String,
    pub dst: String,
    pub msg: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PING {
    pub id: u8,
    pub term: u64,
    pub commit: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct REPLICATE {
    pub id: u8,
    pub term: u64,
    pub off: u64,
    pub age: u64,
    pub commit: u64,
    pub append: Vec<u8>,
    pub rebase: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ACK {
    pub id: u8,
    pub term: u64,
    pub ack: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct REBASE {
    pub id: u8,
    pub term: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UPGRADE {
    pub id: u8,
    pub term: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PROBE {
    pub id: u8,
    pub term: u64,
    pub head: u64,
    pub age: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AVAILABLE {
    pub id: u8,
    pub term: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ADVERTISE {
    pub id: u8,
    pub term: u64,
    pub head: u64,
    pub age: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VOTE {
    pub id: u8,
    pub term: u64,
}
