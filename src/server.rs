use std::io::Cursor;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

use crate::ChronosDb;
use crate::model::VECTOR_DIM;
use crate::cluster::types::{ChronosRaft, ChronosRequest};

// --- OpCodes ---
const OP_INSERT: u8     = 0x01;
const OP_GET: u8        = 0x02;
const OP_SEARCH: u8     = 0x03;
const OP_HISTORY: u8    = 0x04;
const OP_DELETE: u8     = 0x05;
const OP_UPDATE: u8     = 0x06;
const OP_GET_AS_OF: u8  = 0x07;
const OP_COMPACT: u8    = 0x08;

pub struct ChronosServer {
    db: Arc<ChronosDb>,
    raft: ChronosRaft,
}

impl ChronosServer {
    pub fn new(db: Arc<ChronosDb>, raft: ChronosRaft) -> Self {
        Self { db, raft }
    }

    pub async fn run(&self, addr: &str) {
        let listener = TcpListener::bind(addr).await.expect("Could not bind to port");
        println!("ChronosDB (Raft-Enabled) Listening on {}", addr);

        loop {
            match listener.accept().await {
                Ok((socket, _)) => {
                    let db = self.db.clone();
                    let raft = self.raft.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_client(socket, db, raft).await {
                            // Ignore expected disconnections to keep logs clean
                            if e.kind() != std::io::ErrorKind::UnexpectedEof {
                                eprintln!("Client Error: {}", e);
                            }
                        }
                    });
                }
                Err(e) => eprintln!("Connection failed: {}", e),
            }
        }
    }
}

async fn handle_client(mut stream: TcpStream, db: Arc<ChronosDb>, raft: ChronosRaft) -> std::io::Result<()> {
    // 64KB buffer to prevent large-payload DoS
    let mut buffer = [0u8; 65536];

    loop {
        // 1. Read OpCode
        let mut op_buf = [0u8; 1];
        if stream.read_exact(&mut op_buf).await.is_err() {
            return Ok(());
        }
        let op_code = op_buf[0];

        // 2. Read Length
        let mut len_buf = [0u8; 4];
        if stream.read_exact(&mut len_buf).await.is_err() {
            return Ok(());
        }
        let length = u32::from_le_bytes(len_buf) as usize;

        // 3. Read Body
        if length > buffer.len() {
            eprintln!("Payload too large: {} bytes (Max 65536)", length);
            return Ok(());
        }
        stream.read_exact(&mut buffer[..length]).await?;
        let payload = &buffer[..length];

        let mut writer = BufWriter::new(&mut stream);

        // 4. Process Command
        match op_code {
            // Write Operations (Forward to Raft)
            OP_INSERT => handle_insert(&mut writer, payload, &raft).await?,
            OP_DELETE => handle_delete(&mut writer, payload, &raft).await?,
            OP_UPDATE => handle_update(&mut writer, payload, &raft).await?,

            // Read Operations (Local DB)
            OP_GET        => handle_get(&mut writer, payload, &db).await?,
            OP_SEARCH     => handle_search(&mut writer, payload, &db).await?,
            OP_HISTORY    => handle_history(&mut writer, payload, &db).await?,
            OP_GET_AS_OF  => handle_get_as_of(&mut writer, payload, &db).await?,

            // Maintenance
            OP_COMPACT    => handle_compact(&mut writer, payload, &db).await?,

            _ => {
                eprintln!("Unknown OpCode: 0x{:02X}", op_code);
                return Ok(());
            }
        }
        writer.flush().await?;
    }
}

// --- WRITE HANDLERS (RAFT) ---

async fn handle_insert<W: AsyncWriteExt + Unpin>(writer: &mut W, data: &[u8], raft: &ChronosRaft) -> std::io::Result<()> {
    let vec_size = VECTOR_DIM * 4;
    if data.len() < 16 + vec_size {
        writer.write_all(b"ER").await?;
        return Ok(());
    }

    let mut cursor = Cursor::new(data);

    let mut uuid_bytes = [0u8; 16];
    std::io::Read::read_exact(&mut cursor, &mut uuid_bytes).unwrap();
    let id = Uuid::from_bytes(uuid_bytes);

    let mut vector = Vec::with_capacity(VECTOR_DIM);
    let mut f32_buf = [0u8; 4];
    for _ in 0..VECTOR_DIM {
        std::io::Read::read_exact(&mut cursor, &mut f32_buf).unwrap();
        vector.push(f32::from_le_bytes(f32_buf));
    }

    let payload_pos = cursor.position() as usize;
    let payload = data[payload_pos..].to_vec();

    // Capture deterministic timestamp for Raft State Machine
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

    let req = ChronosRequest::Insert { id, vector, payload, ts };

    match raft.client_write(req).await {
        Ok(_) => writer.write_all(b"OK").await?,
        Err(e) => {
            eprintln!("Raft Write Error: {:?}", e);
            writer.write_all(b"ER").await?;
        }
    }
    Ok(())
}

async fn handle_update<W: AsyncWriteExt + Unpin>(writer: &mut W, data: &[u8], raft: &ChronosRaft) -> std::io::Result<()> {
    if data.len() < 16 {
        writer.write_all(b"ER").await?;
        return Ok(());
    }

    let id = Uuid::from_bytes(data[..16].try_into().unwrap());
    let payload = data[16..].to_vec();
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

    let req = ChronosRequest::Update { id, payload, ts };

    match raft.client_write(req).await {
        Ok(_) => writer.write_all(b"OK").await?,
        Err(_) => writer.write_all(b"ER").await?,
    }
    Ok(())
}

async fn handle_delete<W: AsyncWriteExt + Unpin>(writer: &mut W, data: &[u8], raft: &ChronosRaft) -> std::io::Result<()> {
    if data.len() != 16 { return Ok(()); }
    let id = Uuid::from_bytes(data.try_into().unwrap());

    let req = ChronosRequest::Delete { id };

    match raft.client_write(req).await {
        Ok(_) => writer.write_all(b"OK").await?,
        Err(_) => writer.write_all(b"ER").await?,
    }
    Ok(())
}

// --- READ HANDLERS (LOCAL DB) ---

async fn handle_get<W: AsyncWriteExt + Unpin>(writer: &mut W, data: &[u8], db: &Arc<ChronosDb>) -> std::io::Result<()> {
    if data.len() != 16 { return Ok(()); }
    let id = Uuid::from_bytes(data.try_into().unwrap());

    if let Some(record) = db.get_latest(id) {
        writer.write_all(&[1u8]).await?; // Found
        let len = (record.payload.len() as u32).to_le_bytes();
        writer.write_all(&len).await?;
        writer.write_all(&record.payload).await?;
    } else {
        writer.write_all(&[0u8]).await?; // Not Found
    }
    Ok(())
}

async fn handle_get_as_of<W: AsyncWriteExt + Unpin>(writer: &mut W, data: &[u8], db: &Arc<ChronosDb>) -> std::io::Result<()> {
    // Protocol: [UUID (16b)] [Timestamp u64 (8b)]
    if data.len() != 24 {
        writer.write_all(&[0u8]).await?;
        return Ok(());
    }

    let mut cursor = Cursor::new(data);
    let mut uuid_bytes = [0u8; 16];
    std::io::Read::read_exact(&mut cursor, &mut uuid_bytes).unwrap();
    let id = Uuid::from_bytes(uuid_bytes);

    let mut ts_bytes = [0u8; 8];
    std::io::Read::read_exact(&mut cursor, &mut ts_bytes).unwrap();
    let target_time = u64::from_le_bytes(ts_bytes);

    if let Some(record) = db.get_as_of(id, target_time) {
        writer.write_all(&[1u8]).await?;
        let len = (record.payload.len() as u32).to_le_bytes();
        writer.write_all(&len).await?;
        writer.write_all(&record.payload).await?;
    } else {
        writer.write_all(&[0u8]).await?;
    }
    Ok(())
}

async fn handle_search<W: AsyncWriteExt + Unpin>(writer: &mut W, data: &[u8], db: &Arc<ChronosDb>) -> std::io::Result<()> {
    let mut cursor = Cursor::new(data);

    let mut k_buf = [0u8; 4];
    if std::io::Read::read_exact(&mut cursor, &mut k_buf).is_err() { return Ok(()); }
    let k = u32::from_le_bytes(k_buf) as usize;

    let mut query = Vec::with_capacity(VECTOR_DIM);
    let mut f32_buf = [0u8; 4];
    for _ in 0..VECTOR_DIM {
        if std::io::Read::read_exact(&mut cursor, &mut f32_buf).is_err() { return Ok(()); }
        query.push(f32::from_le_bytes(f32_buf));
    }

    let results = db.vector_index.search(&query, k);

    let count = (results.len() as u32).to_le_bytes();
    writer.write_all(&count).await?;

    for (node_id, dist_sq) in results {
        let uuid = Uuid::from_u128(node_id);
        writer.write_all(uuid.as_bytes()).await?;
        writer.write_all(&dist_sq.to_le_bytes()).await?;
    }
    Ok(())
}

async fn handle_history<W: AsyncWriteExt + Unpin>(writer: &mut W, data: &[u8], db: &Arc<ChronosDb>) -> std::io::Result<()> {
    if data.len() != 16 { return Ok(()); }
    let id = Uuid::from_bytes(data.try_into().unwrap());

    let history = db.get_history(id);

    let count = (history.len() as u32).to_le_bytes();
    writer.write_all(&count).await?;

    for record in history {
        writer.write_all(&record.valid_time.start.to_le_bytes()).await?;
        writer.write_all(&record.valid_time.end.to_le_bytes()).await?;

        let p_len = (record.payload.len() as u32).to_le_bytes();
        writer.write_all(&p_len).await?;
        writer.write_all(&record.payload).await?;
    }
    Ok(())
}

async fn handle_compact<W: AsyncWriteExt + Unpin>(writer: &mut W, data: &[u8], db: &Arc<ChronosDb>) -> std::io::Result<()> {
    // Protocol: [History Limit u64 (8b)]
    if data.len() != 8 {
        writer.write_all(b"ER").await?;
        return Ok(());
    }

    let limit = u64::from_le_bytes(data.try_into().unwrap()) as usize;

    // Spawn blocking task for heavy I/O. Map error to string for thread safety.
    let db_clone = db.clone();
    let res = tokio::task::spawn_blocking(move || {
        db_clone.compact(limit).map_err(|e| e.to_string())
    }).await;

    match res {
        Ok(Ok(_)) => writer.write_all(b"OK").await?,
        Ok(Err(e)) => {
            eprintln!("Compaction Failed: {}", e);
            writer.write_all(b"ER").await?;
        },
        Err(e) => {
            eprintln!("Compaction Task Error: {}", e);
            writer.write_all(b"ER").await?;
        }
    }
    Ok(())
}
