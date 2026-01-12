// src/cluster/types.rs
use std::io::Cursor;
use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type NodeId = u64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ChronosRequest {
    Insert {
        id: Uuid,
        vector: Vec<f32>,
        payload: Vec<u8>,
        ts: u64
    },
    Delete {
        id: Uuid
    },
    Update {
        id: Uuid,
        payload: Vec<u8>,
        ts: u64
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChronosResponse {
    pub success: bool,
    pub message: String,
}

openraft::declare_raft_types!(
    pub TypeConfig:
    D = ChronosRequest,
    R = ChronosResponse,
    NodeId = NodeId,
    Node = BasicNode,
    SnapshotData = Cursor<Vec<u8>>
);

pub type ChronosRaft = openraft::Raft<TypeConfig>;
