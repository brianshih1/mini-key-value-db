use uuid::Uuid;

use super::Key;

/**
//  * Modeled after CockroachDB's https://github.com/cockroachdb/cockroach/blob/master/pkg/storage/engine_key.go#L24
//  */
// struct EngineKey {
//     key: Key,
//     version: Vec<u8>,
// }

pub type LockStrength = u32;
pub const None: LockStrength = 0;
pub const Shared: LockStrength = 1;
pub const Upgrade: LockStrength = 2;
pub const Exclusive: LockStrength = 3;

struct LockTableKey {
    key: Key,
    strength: LockStrength,
    txn_id: Uuid, // TODO: slice?
}
