use bincode::serialize;

macro_rules! declare {
    ($code:expr, $msg:ident) => {
        impl $msg {
            pub const CODE: u8 = $code;
            pub fn to_bytes(&self, term: u64) -> Vec<u8> {
                let slot = SLOT {
                    code: $msg::CODE,
                    term,
                    blob: serialize(&self).unwrap(),
                };
                serialize(&slot).unwrap()
            }
        }
    };
}

declare!(0, NULL);
declare!(1, LABEL);

#[derive(Debug, Serialize, Deserialize)]
pub struct SLOT {
    pub term: u64,
    pub code: u8,
    pub blob: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NULL {}

#[derive(Debug, Serialize, Deserialize)]
pub struct LABEL {
    pub text: String,
}
