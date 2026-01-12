use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write, Read};
use std::path::{Path, PathBuf};
use memmap2::MmapMut;
use crate::model::Record;
use rkyv::Deserialize;

// Constants
const SEGMENT_SIZE: u64 = 64 * 1024 * 1024; // 64 MB

#[derive(Debug)]
pub struct Segment {
    pub file_path: PathBuf,
    file: File,
    mmap: Option<MmapMut>,
    current_offset: u64,
}

impl Segment {
    pub fn new(path: &Path, _strict: bool) -> io::Result<Self> {
        let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;

        let metadata = file.metadata()?;
        let current_offset = metadata.len();

        Ok(Self {
            file_path: path.to_path_buf(),
           file,
           // We use standard IO instead of mmap here to simplify concurrency
           // during Garbage Collection (Copy-GC) operations.
           mmap: None,
           current_offset,
        })
    }

    pub fn append(&mut self, record: &Record) -> io::Result<u64> {
        // Use rkyv for zero-copy aligned serialization
        let bytes = rkyv::to_bytes::<_, 4096>(record)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let start = self.current_offset;

        // Length-prefixed write format: [Length (4b)][Data (N bytes)]
        let len = bytes.len() as u32;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&bytes)?;

        self.current_offset += 4 + bytes.len() as u64;
        Ok(start)
    }

    pub fn read(&self, offset: u64) -> io::Result<Record> {
        // Clone file handle for thread-safe read (avoids seeking the writer)
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(offset))?;

        let mut len_buf = [0u8; 4];
        file.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as usize;

        let mut bytes = vec![0u8; len];
        file.read_exact(&mut bytes)?;

        // Deserialize using rkyv
        let archived = unsafe { rkyv::archived_root::<Record>(&bytes) };
        let record: Record = archived.deserialize(&mut rkyv::Infallible).unwrap();

        Ok(record)
    }
}
