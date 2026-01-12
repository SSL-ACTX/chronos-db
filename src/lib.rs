pub mod model;
pub mod storage;
pub mod vector;
pub mod index;
pub mod server;
pub mod parser;
pub mod filter;
pub mod cluster;
pub mod manager;

use std::collections::HashMap;
use std::sync::{Mutex, RwLock};
use std::fmt;
use std::fs;
use uuid::Uuid;
use crate::model::Record;
use crate::storage::Segment;
use crate::index::HnswIndex;
use crate::filter::BloomFilter;

use rkyv::ser::{serializers::AllocSerializer, Serializer};
use rkyv::Deserialize;

pub struct ChronosDb {
    active_segment: Mutex<Segment>,
    pub index: RwLock<HashMap<u128, Vec<u64>>>,
    pub vector_index: HnswIndex,
    pub bloom_filter: RwLock<BloomFilter>,
}

impl fmt::Debug for ChronosDb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChronosDb")
        .field("index_count", &self.index.read().unwrap().len())
        .finish()
    }
}

impl ChronosDb {
    pub fn new(storage_path: &std::path::Path, index_path: &std::path::Path, strict_durability: bool) -> Self {
        let segment = Segment::new(storage_path, strict_durability).expect("Failed to initialize storage");
        let idx = HashMap::new();

        let vector_index = if index_path.exists() {
            println!("Loading HNSW Graph from disk...");
            HnswIndex::load(index_path, 16, 100).unwrap_or_else(|e| {
                println!("Failed to load graph: {}, creating new.", e);
                HnswIndex::new(16, 100)
            })
        } else {
            HnswIndex::new(16, 100)
        };

        let bloom_filter = RwLock::new(BloomFilter::new(1_000_000, 0.01));

        Self {
            active_segment: Mutex::new(segment),
            index: RwLock::new(idx),
            vector_index,
            bloom_filter,
        }
    }

    pub fn insert(&self, record: Record) -> Result<(), String> {
        let id = record.key;
        let vector_clone = record.vector.clone();

        let offset = {
            let mut segment = self.active_segment.lock().map_err(|_| "Poisoned Lock")?;
            segment.append(&record).map_err(|e| e.to_string())?
        };

        {
            let mut bf = self.bloom_filter.write().map_err(|_| "Poisoned Lock")?;
            bf.insert(&id.to_le_bytes());
        }

        {
            let mut idx = self.index.write().map_err(|_| "Poisoned Lock")?;
            idx.entry(id).or_insert_with(Vec::new).push(offset);
        }

        self.vector_index.insert(id, vector_clone);

        Ok(())
    }

    pub fn delete(&self, id: Uuid) -> Result<(), String> {
        {
            let mut idx = self.index.write().map_err(|_| "Poisoned Lock")?;
            idx.remove(&id.as_u128());
        }
        self.vector_index.remove(id.as_u128());
        Ok(())
    }

    pub fn get_latest(&self, id: Uuid) -> Option<Record> {
        {
            let bf = self.bloom_filter.read().ok()?;
            if !bf.contains(&id.as_u128().to_le_bytes()) {
                return None;
            }
        }

        let offset = {
            let idx = self.index.read().ok()?;
            *idx.get(&id.as_u128())?.last()?
        };
        self.read_record(offset)
    }

    pub fn get_as_of(&self, id: Uuid, target_time: u64) -> Option<Record> {
        {
            let bf = self.bloom_filter.read().ok()?;
            if !bf.contains(&id.as_u128().to_le_bytes()) {
                return None;
            }
        }

        let offsets = {
            let idx = self.index.read().ok()?;
            idx.get(&id.as_u128())?.clone()
        };

        for offset in offsets.iter().rev() {
            if let Some(record) = self.read_record(*offset) {
                if record.valid_time.start <= target_time && target_time < record.valid_time.end {
                    return Some(record);
                }
            }
        }
        None
    }

    pub fn get_history(&self, id: Uuid) -> Vec<Record> {
        {
            if let Ok(bf) = self.bloom_filter.read() {
                if !bf.contains(&id.as_u128().to_le_bytes()) {
                    return vec![];
                }
            }
        }

        let offsets = {
            let idx = self.index.read().ok();
            if idx.is_none() { return vec![]; }
            match idx.unwrap().get(&id.as_u128()) {
                Some(list) => list.clone(),
                None => return vec![],
            }
        };

        let mut history = Vec::new();
        for offset in offsets {
            if let Some(record) = self.read_record(offset) {
                history.push(record);
            }
        }
        history
    }

    fn read_record(&self, offset: u64) -> Option<Record> {
        let segment = self.active_segment.lock().ok()?;
        segment.read(offset).ok()
    }

    // --- SNAPSHOTS ---

    pub fn snapshot(&self) -> Result<Vec<u8>, std::io::Error> {
        let index = self.index.read().unwrap();
        let mut records = Vec::new();
        for (_key, offsets) in index.iter() {
            if let Some(last_offset) = offsets.last() {
                if let Some(record) = self.read_record(*last_offset) {
                    records.push(record);
                }
            }
        }

        println!("[SNAPSHOT] Serializing {} records (Binary/rkyv)...", records.len());

        let mut serializer = AllocSerializer::<4096>::default();
        serializer.serialize_value(&records)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        let bytes = serializer.into_serializer().into_inner();
        Ok(bytes.into_vec())
    }

    pub fn restore(&self, snapshot_data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        println!("[RESTORE] Reading binary snapshot ({} bytes)...", snapshot_data.len());

        let mut aligned = rkyv::AlignedVec::with_capacity(snapshot_data.len());
        aligned.extend_from_slice(snapshot_data);

        let archived = unsafe { rkyv::archived_root::<Vec<Record>>(&aligned) };
        let records: Vec<Record> = archived.deserialize(&mut rkyv::Infallible).unwrap();

        let count = records.len();
        println!("[RESTORE] Hydrating {} records...", count);

        {
            let mut idx = self.index.write().unwrap();
            idx.clear();
        }
        {
            let mut bf = self.bloom_filter.write().unwrap();
            *bf = BloomFilter::new(1_000_000, 0.01);
        }
        self.vector_index.clear();

        for record in records {
            self.insert(record).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        }

        println!("[RESTORE] Success.");
        Ok(())
    }

    pub fn compact(&self, history_limit: usize) -> Result<(), Box<dyn std::error::Error>> {
        println!("[GC] Starting Compaction (Retention: Last {} versions)...", history_limit);

        // 1. GLOBAL LOCK (Stop-the-World)
        let mut index_lock = self.index.write().map_err(|_| "Poisoned Index Lock")?;
        let mut segment_lock = self.active_segment.lock().map_err(|_| "Poisoned Segment Lock")?;

        // 2. Prepare New Segment
        let old_path = segment_lock.file_path.clone();
        let new_path = old_path.with_extension("compacted");

        let mut new_segment = Segment::new(&new_path, true)?;

        // 3. Iterate & Copy Live Data
        let mut new_index_map: HashMap<u128, Vec<u64>> = HashMap::new();
        let mut moved_count = 0;
        let mut dropped_count = 0;

        for (key, offsets) in index_lock.iter() {
            // Retention Policy
            let start_idx = if offsets.len() > history_limit {
                dropped_count += offsets.len() - history_limit;
                offsets.len() - history_limit
            } else {
                0
            };

            let offsets_to_keep = &offsets[start_idx..];
            let mut new_offsets = Vec::new();

            for &old_offset in offsets_to_keep {
                if let Ok(record) = segment_lock.read(old_offset) {
                    let new_offset = new_segment.append(&record)?;
                    new_offsets.push(new_offset);
                    moved_count += 1;
                }
            }
            new_index_map.insert(*key, new_offsets);
        }

        // 4. Atomic Swap
        *index_lock = new_index_map;
        *segment_lock = new_segment;

        // 5. Cleanup Disk
        if fs::remove_file(&old_path).is_ok() {
            fs::rename(&new_path, &old_path)?;
            // Re-open at original path to maintain consistent file handle
            *segment_lock = Segment::new(&old_path, true)?;
        }

        println!("[GC] Compaction Complete.");
        println!("     - Moved (Live): {}", moved_count);
        println!("     - Dropped (Dead/Old): {}", dropped_count);

        Ok(())
    }
}
