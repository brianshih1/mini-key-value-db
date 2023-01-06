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
    use serde::{Deserialize, Serialize};

    use super::MVCCMetadata;

    #[derive(Debug, Serialize, Deserialize)]
    struct Test {
        foo: bool,
    }

    #[test]
    fn test() {
        let mvcc = Test { foo: true };
        let str = serde_json::to_string(&mvcc).unwrap();
        let meta = serde_json::from_str::<Test>(&str).unwrap();
        let huh = "";

        let test_bool = true;
        let bool_str = serde_json::to_string(&test_bool).unwrap();
        let meta = serde_json::from_str::<bool>(&bool_str).unwrap();
        // println!("value: {:?}", meta);

        let vec = serde_json::to_vec(&test_bool).unwrap();
        let back = serde_json::from_slice::<bool>(&vec).unwrap();
        // println!("value: {:?}", back);

        let str = "foo";
        let vec = serde_json::to_vec(&str).unwrap();
        let back = serde_json::from_slice::<&str>(&vec).unwrap();
        println!("value: {:?}", back);
        // serde_json::from_slice(value)
    }
}
