use std::collections::BTreeMap;
use std::sync::Arc;
use std::io::Cursor;

use openraft::{
    storage::{LogState, Snapshot},
    BasicNode, Entry, EntryPayload, LogId, RaftLogReader, RaftSnapshotBuilder,
    RaftStorage, StorageError, SnapshotMeta,
    Vote, StoredMembership
};
use tokio::sync::RwLock;
use crate::ChronosDb;
use crate::model::Record;
use super::types::{ChronosRequest, ChronosResponse, TypeConfig};

#[derive(Clone, Debug)]
pub struct ChronosStore {
    current_snapshot: Arc<RwLock<Option<Snapshot<TypeConfig>>>>,
    last_purged_log_id: Arc<RwLock<Option<LogId<u64>>>>,
    log: Arc<RwLock<BTreeMap<u64, Entry<TypeConfig>>>>,
    vote: Arc<RwLock<Option<Vote<u64>>>>,
    stored_membership: Arc<RwLock<StoredMembership<u64, BasicNode>>>,
    db: Arc<ChronosDb>,
}

impl ChronosStore {
    pub fn new(db: Arc<ChronosDb>) -> Self {
        Self {
            current_snapshot: Arc::new(RwLock::new(None)),
            last_purged_log_id: Arc::new(RwLock::new(None)),
            log: Arc::new(RwLock::new(BTreeMap::new())),
            vote: Arc::new(RwLock::new(None)),
            stored_membership: Arc::new(RwLock::new(Default::default())),
            db,
        }
    }
}

// --- TRAIT 1: RaftStorage ---
impl RaftStorage<TypeConfig> for ChronosStore {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_reader(&mut self) -> Self::LogReader { self.clone() }
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder { self.clone() }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let log = self.log.read().await;
        let last_purged = *self.last_purged_log_id.read().await;
        let last_log = log.iter().last().map(|(_, ent)| ent.log_id);
        let last_log_id = last_log.or(last_purged);
        Ok(LogState { last_purged_log_id: last_purged, last_log_id })
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        *self.vote.write().await = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(*self.vote.read().await)
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<u64>>
    where I: IntoIterator<Item = Entry<TypeConfig>> {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut log = self.log.write().await;
        let keys: Vec<u64> = log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in keys { log.remove(&key); }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut log = self.log.write().await;
        *self.last_purged_log_id.write().await = Some(log_id);
        let keys: Vec<u64> = log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in keys { log.remove(&key); }
        Ok(())
    }

    async fn last_applied_state(&mut self) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let membership = self.stored_membership.read().await.clone();
        Ok((None, membership))
    }

    async fn apply_to_state_machine(&mut self, entries: &[Entry<TypeConfig>]) -> Result<Vec<ChronosResponse>, StorageError<u64>> {
        let mut responses = Vec::with_capacity(entries.len());

        for entry in entries {
            match &entry.payload {
                // 1. Data Operations
                EntryPayload::Normal(req) => {
                    match req {
                        // Insert
                        ChronosRequest::Insert { id, vector, payload, ts } => {
                            let r = Record::new(*id, vector.clone(), payload.clone(), *ts);
                            let db = self.db.clone();
                            let _ = tokio::task::spawn_blocking(move || db.insert(r)).await;
                            responses.push(ChronosResponse { success: true, message: "OK".into() });
                        }

                        // Update
                        ChronosRequest::Update { id, payload, ts } => {
                            let db = self.db.clone();
                            let payload_clone = payload.clone();
                            let ts_val = *ts;
                            let id_val = *id;

                            let _ = tokio::task::spawn_blocking(move || {
                                if let Some(old_record) = db.get_latest(id_val) {
                                    let new_record = Record::new(id_val, old_record.vector, payload_clone, ts_val);
                                    db.insert(new_record)
                                } else {
                                    Err("ID not found for Update".to_string())
                                }
                            }).await;
                            responses.push(ChronosResponse { success: true, message: "OK".into() });
                        }

                        // Delete
                        ChronosRequest::Delete { id } => {
                            let db = self.db.clone();
                            let id_val = *id;
                            let _ = tokio::task::spawn_blocking(move || db.delete(id_val)).await;
                            responses.push(ChronosResponse { success: true, message: "OK".into() });
                        }
                    }
                }

                // 2. Membership Changes
                EntryPayload::Membership(mem) => {
                    let mut stored = self.stored_membership.write().await;
                    *stored = StoredMembership::new(Some(entry.log_id), mem.clone());
                    responses.push(ChronosResponse { success: true, message: "Membership Change".into() });
                }

                // 3. Blank / Heartbeats
                EntryPayload::Blank => {
                    responses.push(ChronosResponse { success: true, message: "Heartbeat".into() });
                }
            }
        }
        Ok(responses)
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(&mut self, meta: &SnapshotMeta<u64, BasicNode>, snapshot: Box<Cursor<Vec<u8>>>) -> Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();
        let db = self.db.clone();
        let data_clone = data.clone();

        tokio::task::spawn_blocking(move || {
            db.restore(&data_clone).expect("Failed to restore snapshot");
        }).await.unwrap();

        *self.stored_membership.write().await = meta.last_membership.clone();
        *self.last_purged_log_id.write().await = meta.last_log_id;

        *self.current_snapshot.write().await = Some(Snapshot {
            meta: meta.clone(),
                                                    snapshot: Box::new(Cursor::new(data)),
        });

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        Ok(self.current_snapshot.read().await.clone())
    }
}

// --- TRAIT 2: RaftLogReader ---
impl RaftLogReader<TypeConfig> for ChronosStore {
    async fn try_get_log_entries<R>(&mut self, range: R) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>>
    where R: std::ops::RangeBounds<u64> {
        let log = self.log.read().await;
        Ok(log.range(range).map(|(_, v)| v.clone()).collect())
    }
}

// --- TRAIT 3: RaftSnapshotBuilder ---
impl RaftSnapshotBuilder<TypeConfig> for ChronosStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let db = self.db.clone();

        // Create full DB snapshot serialization
        let data = tokio::task::spawn_blocking(move || {
            db.snapshot().expect("Failed to create snapshot")
        }).await.unwrap();

        let last_log_id = self.log.read().await.iter().last().map(|(_, e)| e.log_id).unwrap_or_default();
        let membership = self.stored_membership.read().await.clone();

        let snapshot = Snapshot {
            meta: SnapshotMeta {
                last_log_id: Some(last_log_id),
                last_membership: membership,
                snapshot_id: uuid::Uuid::new_v4().to_string(),
            },
            snapshot: Box::new(Cursor::new(data)),
        };

        *self.current_snapshot.write().await = Some(snapshot.clone());
        Ok(snapshot)
    }
}
