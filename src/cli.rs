use std::io::{self, Write, Read};
use std::net::TcpStream;
use uuid::Uuid;
use chronos::parser::{self, Command};

const HOST: &str = "127.0.0.1:9000";
const VECTOR_DIM: usize = 128;

// OpCodes
const OP_INSERT: u8     = 0x01;
const OP_GET: u8        = 0x02;
const OP_SEARCH: u8     = 0x03;
const OP_HISTORY: u8    = 0x04;
const OP_DELETE: u8     = 0x05;
const OP_GET_AS_OF: u8  = 0x07;

fn main() {
    print_banner();

    match TcpStream::connect(HOST) {
        Ok(_) => println!("[\u{2713}] Connected to ChronosDB at {}!", HOST),
        Err(_) => {
            println!("[\u{2717}] Could not connect to server at {}.", HOST);
            println!("    Make sure to run 'cargo run --release' in another terminal.");
            return;
        }
    }
    println!("Type 'HELP' for supported commands or 'EXIT' to quit.\n");

    let stdin = io::stdin();
    let mut buffer = String::new();

    loop {
        print!("chronos> ");
        io::stdout().flush().unwrap();
        buffer.clear();

        if stdin.read_line(&mut buffer).unwrap() == 0 { break; }
        if buffer.trim().is_empty() { continue; }

        match parser::parse_command(&buffer) {
            Ok(cmd) => {
                if let Err(e) = execute_command(cmd) {
                    println!("[\u{26a0}\u{fe0f} Error] {}", e);
                }
            }
            Err(e) => {
                println!("[\u{2717} Syntax Error] {}", e);
                if buffer.contains("...") {
                    println!("    \u{2139}\u{fe0f}  Hint: Ellipses (...) are not supported. Please close the list: [0.1, 0.5]");
                } else if buffer.to_uppercase().starts_with("SELECT") {
                    println!("    \u{2139}\u{fe0f}  Hint: Try 'SELECT FROM VECTORS WHERE VECTOR NEAR [0.1, ...] LIMIT 5'");
                }
            }
        }
    }
}

fn print_banner() {
    println!("\n==================================================");
    println!("   ChronosDB CLI v0.5 - The Time-Traveling DB");
    println!("==================================================\n");
}

fn print_help() {
    println!("\n--- Available Commands ---");
    println!("1. INSERT:      INSERT INTO VECTORS VALUES ([0.1, ...], \"payload\")");
    println!("2. SEARCH:      SELECT FROM VECTORS WHERE VECTOR NEAR [0.1, ...] LIMIT 5");
    println!("3. GET:         GET 'uuid'");
    println!("4. HISTORY:     HISTORY 'uuid'");
    println!("5. TIME TRAVEL: SELECT FROM VECTORS WHERE ID='uuid' AS OF 1234567890");
    println!("6. UPDATE:      UPDATE VECTORS SET PAYLOAD=\"new\" WHERE ID='uuid'");
    println!("7. DELETE:      DELETE FROM VECTORS WHERE ID='uuid'");
    println!("8. EXIT:        Quit\n");
}

fn execute_command(cmd: Command) -> Result<(), String> {
    match cmd {
        Command::Help => { print_help(); Ok(()) },
        Command::Insert { vector, payload, id } => perform_insert(vector, payload, id),

        // Route SELECT commands to either Time Travel or Vector Search
        Command::Select { vector, filter_id, as_of, limit } => {
            if let (Some(id), Some(ts)) = (filter_id, as_of) {
                // Case 1: Time Travel Query (ID + AS OF)
                perform_get_as_of(id, ts)
            } else if let Some(vec) = vector {
                // Case 2: Vector Search
                perform_search(vec, limit)
            } else {
                Err("SELECT requires either 'WHERE VECTOR NEAR...' or 'WHERE ID=... AS OF...'".into())
            }
        },

        Command::Update { id, payload, .. } => {
            let dummy_vec = vec![0.0; VECTOR_DIM];
            if let Some(p) = payload {
                perform_insert(dummy_vec, p, Some(id))
            } else {
                Err("Update requires a payload.".into())
            }
        },
        Command::Delete { id } => perform_delete(id),
        Command::Get { id } => perform_get(id),
        Command::History { id } => perform_history(id),
        Command::Exit => std::process::exit(0),
    }
}

// --- NETWORK HANDLERS ---

fn perform_insert(mut vector: Vec<f32>, payload: String, explicit_id: Option<Uuid>) -> Result<(), String> {
    if vector.len() > VECTOR_DIM { return Err(format!("Vector too long (Max {})", VECTOR_DIM)); }
    vector.resize(VECTOR_DIM, 0.0);

    let mut stream = TcpStream::connect(HOST).map_err(|e| e.to_string())?;
    let id = explicit_id.unwrap_or_else(Uuid::new_v4);
    let payload_bytes = payload.as_bytes();
    let total_len = (16 + (VECTOR_DIM * 4) + payload_bytes.len()) as u32;

    stream.write_all(&[OP_INSERT]).unwrap();
    stream.write_all(&total_len.to_le_bytes()).unwrap();
    stream.write_all(id.as_bytes()).unwrap();
    for f in vector { stream.write_all(&f.to_le_bytes()).unwrap(); }
    stream.write_all(payload_bytes).unwrap();

    let mut resp = [0u8; 2];
    stream.read_exact(&mut resp).unwrap();
    if &resp == b"OK" {
        println!("[\u{2713} OK] Inserted ID: {}", id);
        Ok(())
    } else {
        Err("Server Rejected Request".into())
    }
}

fn perform_search(mut vector: Vec<f32>, limit: usize) -> Result<(), String> {
    vector.resize(VECTOR_DIM, 0.0);
    let mut stream = TcpStream::connect(HOST).map_err(|e| e.to_string())?;
    let total_len = (4 + (VECTOR_DIM * 4)) as u32;

    stream.write_all(&[OP_SEARCH]).unwrap();
    stream.write_all(&total_len.to_le_bytes()).unwrap();
    stream.write_all(&(limit as u32).to_le_bytes()).unwrap();
    for f in vector { stream.write_all(&f.to_le_bytes()).unwrap(); }

    let mut count_buf = [0u8; 4];
    stream.read_exact(&mut count_buf).unwrap();
    let count = u32::from_le_bytes(count_buf);

    println!("\nFound {} matches:", count);
    for _ in 0..count {
        let mut uuid_buf = [0u8; 16];
        stream.read_exact(&mut uuid_buf).unwrap();
        let mut dist_buf = [0u8; 4];
        stream.read_exact(&mut dist_buf).unwrap();
        let dist = f32::from_le_bytes(dist_buf).sqrt();
        println!("  • {} (Dist: {:.4})", Uuid::from_bytes(uuid_buf), dist);
    }
    println!();
    Ok(())
}

fn perform_get(id: Uuid) -> Result<(), String> {
    let mut stream = TcpStream::connect(HOST).map_err(|e| e.to_string())?;
    stream.write_all(&[OP_GET]).unwrap();
    stream.write_all(&16u32.to_le_bytes()).unwrap();
    stream.write_all(id.as_bytes()).unwrap();

    let mut found = [0u8; 1];
    stream.read_exact(&mut found).unwrap();

    if found[0] == 1 {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).unwrap();
        println!("Payload: \"{}\"", String::from_utf8_lossy(&payload));
        Ok(())
    } else {
        println!("[\u{2717}] ID Not Found.");
        Ok(())
    }
}

fn perform_get_as_of(id: Uuid, timestamp: u64) -> Result<(), String> {
    let mut stream = TcpStream::connect(HOST).map_err(|e| e.to_string())?;

    // Body: [UUID (16)] [Timestamp (8)]
    // Total Len: 24 bytes
    stream.write_all(&[OP_GET_AS_OF]).unwrap();
    stream.write_all(&24u32.to_le_bytes()).unwrap();
    stream.write_all(id.as_bytes()).unwrap();
    stream.write_all(&timestamp.to_le_bytes()).unwrap();

    let mut found = [0u8; 1];
    stream.read_exact(&mut found).unwrap();

    if found[0] == 1 {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).unwrap();

        println!("[\u{23f1}\u{fe0f} Time Travel] Record state at {}:", timestamp);
        println!("Payload: \"{}\"", String::from_utf8_lossy(&payload));
        Ok(())
    } else {
        println!("[\u{2717}] No record found valid at time {}.", timestamp);
        Ok(())
    }
}

fn perform_history(id: Uuid) -> Result<(), String> {
    let mut stream = TcpStream::connect(HOST).map_err(|e| e.to_string())?;
    stream.write_all(&[OP_HISTORY]).unwrap();
    stream.write_all(&16u32.to_le_bytes()).unwrap();
    stream.write_all(id.as_bytes()).unwrap();

    let mut count_buf = [0u8; 4];
    stream.read_exact(&mut count_buf).unwrap();
    let count = u32::from_le_bytes(count_buf);

    println!("History for {}:", id);
    for i in 0..count {
        let mut start_buf = [0u8; 8];
        stream.read_exact(&mut start_buf).unwrap();
        let mut end_buf = [0u8; 8];
        stream.read_exact(&mut end_buf).unwrap();
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).unwrap();

        let start = u64::from_le_bytes(start_buf);
        let end = u64::from_le_bytes(end_buf);
        let end_str = if end == u64::MAX { "PRESENT".to_string() } else { end.to_string() };
        println!("  v{} | {} -> {} | \"{}\"", i+1, start, end_str, String::from_utf8_lossy(&payload));
    }
    Ok(())
}

fn perform_delete(id: Uuid) -> Result<(), String> {
    let mut stream = TcpStream::connect(HOST).map_err(|e| e.to_string())?;
    stream.write_all(&[OP_DELETE]).unwrap();
    stream.write_all(&16u32.to_le_bytes()).unwrap();
    stream.write_all(id.as_bytes()).unwrap();

    let mut resp = [0u8; 2];
    stream.read_exact(&mut resp).unwrap();
    if &resp == b"OK" {
        println!("[\u{2713} OK] Deleted ID: {}", id);
        Ok(())
    } else {
        Err("Delete Failed".into())
    }
}
