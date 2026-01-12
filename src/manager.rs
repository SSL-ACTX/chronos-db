use std::sync::Arc;
use std::thread;
use std::time::Duration;
use crate::ChronosDb;

#[derive(Debug, Clone, PartialEq)]
pub enum SimdLevel {
    None,
    Sse2,
    Avx2,
    Avx512,
    Neon,
}

impl std::fmt::Display for SimdLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone)]
pub struct SystemProfile {
    pub logical_cores: usize,
    pub worker_threads: usize,
    pub strict_durability: bool, // true = fsync, false = OS buffer (faster)
    pub raft_heartbeat: u64,     // Slower CPUs need higher timeouts
    pub simd_level: SimdLevel,
}

impl SystemProfile {
    pub fn detect() -> Self {
        let cores = thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        let simd_level = Self::detect_simd();

        // CASE 1: Potato Mode (Single Core OR Ancient CPU)
        if cores <= 1 || simd_level == SimdLevel::None {
            println!("[\u{26a0}\u{fe0f}  MANAGER] CPU Constraint Detected (Cores: {}, SIMD: {:?}).", cores, simd_level);
            println!("             Enabling 'Potato Mode' (No Fsync, 1s Timeout).");

            Self {
                logical_cores: cores,
                worker_threads: 2, // 1 Compute + 1 I/O
                strict_durability: false,
                raft_heartbeat: 1000,
                simd_level,
            }
        }
        // CASE 2: Standard Mode (Desktop / Laptop)
        else if cores < 6 {
            let heartbeat = if simd_level == SimdLevel::Avx512 { 300 } else { 500 };

            Self {
                logical_cores: cores,
                worker_threads: cores,
                strict_durability: true,
                raft_heartbeat: heartbeat,
                simd_level,
            }
        }
        // CASE 3: Server Mode (High Core Count)
        else {
            let heartbeat = match simd_level {
                SimdLevel::Avx512 => 100, // Extremely aggressive leadership checks
                SimdLevel::Avx2 | SimdLevel::Neon => 250,
                _ => 500, // Many cores but slow math? Safe fallback.
            };

            Self {
                logical_cores: cores,
                worker_threads: cores,
                strict_durability: true,
                raft_heartbeat: heartbeat,
                simd_level,
            }
        }
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    fn detect_simd() -> SimdLevel {
        if is_x86_feature_detected!("avx512f") {
            SimdLevel::Avx512
        } else if is_x86_feature_detected!("avx2") {
            SimdLevel::Avx2
        } else if is_x86_feature_detected!("sse2") {
            SimdLevel::Sse2
        } else {
            SimdLevel::None
        }
    }

    #[cfg(target_arch = "aarch64")]
    fn detect_simd() -> SimdLevel {
        SimdLevel::Neon
    }

    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64")))]
    fn detect_simd() -> SimdLevel {
        SimdLevel::None
    }
}

/// Starts the Background Garbage Collector thread.
/// Runs compaction every 10 minutes to prune history older than 10 versions.
pub fn start_gc_thread(db: Arc<ChronosDb>) {
    thread::spawn(move || {
        println!("[GC] Background Compaction Thread Started.");
        loop {
            // Run every 10 minutes (600s)
            thread::sleep(Duration::from_secs(600));

            // Retention Policy: Keep last 10 versions of every record
            if let Err(e) = db.compact(10) {
                eprintln!("[GC] Compaction Failed: {}", e);
            }
        }
    });
}
