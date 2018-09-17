use bincode::serialize;

macro_rules! declare {
    ($code:expr, $msg:ident) => {
        impl $msg {
            pub(super) const CODE: u8 = $code;
            pub(super) fn to_bytes(&self, term: u64) -> Vec<u8> {
                let slot = SLOT {
                    code: $msg::CODE,
                    term,
                    bytes: serialize(&self).unwrap(),
                };
                serialize(&slot).unwrap()
            }
        }
    };
}

declare!(0, NULL);

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct SLOT {
    pub(super) term: u64,
    pub(super) code: u8,
    pub(super) bytes: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct NULL {}
