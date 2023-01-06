use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::hlc::timestamp::Timestamp;

#[derive(Serialize, Deserialize)]
pub struct MVCCMetadata {
    txn_meta: TxnMetadata,
}

#[derive(Serialize, Deserialize)]
struct TxnMetadata {
    transaction_id: Uuid,
    timestamp: Timestamp,
}

pub fn mvcc_get() {}

pub fn mvcc_put() {}

#[cfg(test)]
mod tests {
    use super::MVCCMetadata;

    #[test]
    fn test() {
        // let mvcc = MVCCMetadata {
        //     test: "f".to_owned(),
        // };
        // let str = serde_json::to_string(&mvcc).unwrap();
        // let meta = serde_json::from_str::<MVCCMetadata>(&str).unwrap();
        // let huh = "";
    }
}
