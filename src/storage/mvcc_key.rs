use core::str;

use crate::hlc::timestamp::{get_intent_timestamp, Timestamp};

use super::{str_to_key, Key};

/**
 * Versioned Key where the key is the semantic key and
 * the timestamp is the version of the key.
 */

#[derive(Debug, Eq, PartialOrd, Ord, Clone)]
pub struct MVCCKey {
    pub key: Key,
    pub timestamp: Timestamp,
}

impl PartialEq for MVCCKey {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.timestamp == other.timestamp
    }
}

impl MVCCKey {
    pub fn new(key: &str, timestamp: Timestamp) -> Self {
        MVCCKey {
            key: key.as_bytes().to_vec(),
            timestamp,
        }
    }

    pub fn is_intent_key(&self) -> bool {
        self.timestamp.is_intent_timestamp()
    }

    pub fn to_string(&self) -> String {
        let encoded = encode_mvcc_key(self);
        String::from_utf8(encoded).unwrap()
    }

    pub fn to_encoded(&self) -> Vec<u8> {
        encode_mvcc_key(self)
    }

    pub fn create_intent_key_with_str(key: &str) -> MVCCKey {
        create_intent_key(&str_to_key(key))
    }

    pub fn create_intent_key(key: &Key) -> MVCCKey {
        create_intent_key(key)
    }
}

pub fn create_intent_key(key: &Key) -> MVCCKey {
    MVCCKey {
        key: key.to_owned(),
        timestamp: get_intent_timestamp(),
    }
}

// TODO: byte-buffer?
// inspired by CockroachDB's encodeMVCCKey: https://github.com/cockroachdb/cockroach/blob/master/pkg/storage/mvcc_key.go#L161
/**
 * encoded key takes the form:
 * [key] [wall_time] [logical_time]
 * key: (variable length)
 * wall_time: uint64
 * logical_time: uint32
 */
pub fn encode_mvcc_key(mvcc_key: &MVCCKey) -> Vec<u8> {
    let mut key_vec = mvcc_key.key.to_vec();
    let timestamp_vec = encode_timestamp(mvcc_key.timestamp);
    key_vec.extend(timestamp_vec);
    key_vec
}

pub fn encode_timestamp(timestamp: Timestamp) -> Vec<u8> {
    let mut wall_time_bytes = timestamp.wall_time.to_le_bytes().to_vec();
    let logical_time_bytes = timestamp.logical_time.to_le_bytes().to_vec();
    wall_time_bytes.extend(logical_time_bytes);
    wall_time_bytes
}

pub fn decode_mvcc_key(encoded_mvcc_key: &Vec<u8>) -> Option<MVCCKey> {
    // just a hack
    if encoded_mvcc_key.len() < 12 {
        return None;
    }
    let timestamp_len = 12; // 4 + 8 bytes
    let encoded_mvcc_key_len = encoded_mvcc_key.len();
    let key_end = encoded_mvcc_key_len - timestamp_len;
    let encoded_key = encoded_mvcc_key[..key_end].to_owned();
    let encoded_timestamp = encoded_mvcc_key[key_end..].to_owned();
    let timestamp = decode_timestamp(encoded_timestamp);
    Some(MVCCKey {
        key: encoded_key,
        timestamp: timestamp,
    })
}

fn decode_timestamp(encoded_timestamp: Vec<u8>) -> Timestamp {
    let wall_time = u64::from_le_bytes(encoded_timestamp[0..8].try_into().unwrap());
    let logical_time = u32::from_le_bytes(encoded_timestamp[8..12].try_into().unwrap());

    Timestamp {
        wall_time,
        logical_time,
    }
}

#[cfg(test)]
mod tests {
    use crate::{hlc::timestamp::Timestamp, storage::mvcc_key::decode_mvcc_key};

    use super::{encode_mvcc_key, MVCCKey};
    #[test]
    fn encode_decode_mvcc_key() {
        let mvcc_key = MVCCKey {
            key: "hello".as_bytes().to_vec(),
            timestamp: Timestamp {
                logical_time: 12,
                wall_time: 12,
            },
        };

        let encoded = encode_mvcc_key(&mvcc_key);
        let decoded = decode_mvcc_key(&encoded);
        assert_eq!(mvcc_key, decoded.unwrap());
    }
}
