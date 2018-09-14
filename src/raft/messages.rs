#[derive(Debug, Serialize, Deserialize)]
pub struct RAW {
    pub code: u8,
    pub src: String,
    pub dst: String,
    pub msg: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub blob: String,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct APPEND {
    pub id: u8,
    pub term: u64,
    pub blob: String,
}
