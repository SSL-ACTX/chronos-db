use clap::Parser;
use std::sync::Arc;
use std::path::Path;
use chronos::ChronosDb;
use chronos::cluster::store::ChronosStore;
use chronos::cluster::network::ChronosNetwork;
use chronos::cluster::api::start_raft_api;
use chronos::server::ChronosServer;
use chronos::manager::{self, SystemProfile};
use openraft::{Config, Raft, SnapshotPolicy};
use openraft::storage::Adaptor;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, default_value = "1")]
    node_id: u64,

    #[clap(long, default_value = "127.0.0.1:9000")]
    addr: String,

    #[clap(long, default_value = "20001")]
    raft_port: u16,
}

fn main() {
    let profile = SystemProfile::detect();

    println!("--- [Chronos Resource Manager] ---");
    println!("Detected Cores: {}", profile.logical_cores);
    println!("Worker Threads: {}", profile.worker_threads);
    println!("Durability Mode: {}", if profile.strict_durability { "Strict (Fsync)" } else { "High Throughput (Async)" });
    println!("Raft Heartbeat: {}ms", profile.raft_heartbeat);
    println!("----------------------------------");

    tokio::runtime::Builder::new_multi_thread()
    .worker_threads(profile.worker_threads)
    .enable_all()
    .build()
    .unwrap()
    .block_on(async_main(profile));
}

async fn async_main(profile: SystemProfile) {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info,chronos=info,openraft=info");
    }
    tracing_subscriber::fmt()
    .with_target(false)
    .with_level(true)
    .init();

    let args = Args::parse();
    println!("--- ChronosDB Cluster Node {} ---", args.node_id);

    let wal_file = format!("node_{}_wal.dat", args.node_id);
    let index_file = format!("node_{}_index.dat", args.node_id);

    let storage_path = Path::new(&wal_file);
    let index_path = Path::new(&index_file);

    println!("Initializing Storage Engine...");
    let db = Arc::new(ChronosDb::new(storage_path, index_path, profile.strict_durability));

    manager::start_gc_thread(db.clone());

    println!("Initializing Raft Consensus...");

    let config = Config {
        heartbeat_interval: profile.raft_heartbeat,
        election_timeout_min: profile.raft_heartbeat * 3,
        election_timeout_max: profile.raft_heartbeat * 6,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(20),
        max_in_snapshot_log_to_keep: 0,
        ..Default::default()
    };

    let store = ChronosStore::new(db.clone());
    let network = ChronosNetwork::new();

    let (log_store, state_machine) = Adaptor::new(store.clone());

    let raft = Raft::new(
        args.node_id,
        config.into(),
                         network,
                         log_store,
                         state_machine
    )
    .await
    .expect("Failed to create Raft node");

    let raft_api = raft.clone();
    let raft_port = args.raft_port;
    tokio::spawn(async move {
        start_raft_api(raft_api, raft_port).await;
    });
    println!("Raft HTTP API listening on port {}", raft_port);

    let addr = args.addr.clone();
    let db_clone = db.clone();
    let raft_clone = raft.clone();

    tokio::spawn(async move {
        let server = ChronosServer::new(db_clone, raft_clone);
        server.run(&addr).await;
    });

    println!("ChronosDB Client Server listening on {}", args.addr);
    println!("Node is Ready.");

    tokio::signal::ctrl_c().await.unwrap();
    println!("Shutting down.");
}
