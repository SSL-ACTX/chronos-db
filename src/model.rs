use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Configurable dimensionality (128 is efficient for general purpose)
pub const VECTOR_DIM: usize = 128;

/// Time travel coordinates
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
#[archive(check_bytes)]
pub struct TimeStamp {
    pub start: u64, // Unix Timestamp
    pub end: u64,   // u64::MAX for "currently valid"
}

/// The Atomic Unit of Chronos
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Serialize, Deserialize, Debug, PartialEq)]
#[archive(check_bytes)]
pub struct Record {
    /// Unique ID of the entity
    pub key: u128,

    /// The high-dimensional embedding (The "meaning")
    pub vector: Vec<f32>,

    /// Raw binary payload
    pub payload: Vec<u8>,

    /// When this fact was true in the real world
    pub valid_time: TimeStamp,

    /// When this fact was recorded in the database
    pub tx_time: u64,
}

impl Record {
    pub fn new(key: Uuid, vector: Vec<f32>, payload: Vec<u8>, ts: u64) -> Self {
        Self {
            key: key.as_u128(),
            vector,
            payload,
            // Start time is explicit (provided by Raft log) ensuring deterministic history
            valid_time: TimeStamp { start: ts, end: u64::MAX },
            tx_time: ts,
        }
    }
}
