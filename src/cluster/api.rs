use std::sync::Arc;
use warp::Filter;
use crate::cluster::types::ChronosRaft;
use crate::cluster::types::NodeId;
use openraft::BasicNode;
use std::time::Duration;

pub async fn start_raft_api(raft: ChronosRaft, port: u16) {
    let raft = Arc::new(raft);

    // 1. POST /raft-vote
    let vote = warp::post()
    .and(warp::path("raft-vote"))
    .and(warp::body::json())
    .and(with_raft(raft.clone()))
    .and_then(|req, raft: Arc<ChronosRaft>| async move {
        let res = raft.vote(req).await;
        Ok::<_, warp::Rejection>(warp::reply::json(&res.unwrap()))
    });

    // 2. POST /raft-append
    let append = warp::post()
    .and(warp::path("raft-append"))
    .and(warp::body::json())
    .and(with_raft(raft.clone()))
    .and_then(|req, raft: Arc<ChronosRaft>| async move {
        let res = raft.append_entries(req).await;
        Ok::<_, warp::Rejection>(warp::reply::json(&res.unwrap()))
    });

    // 3. POST /raft-snapshot
    let snapshot = warp::post()
    .and(warp::path("raft-snapshot"))
    .and(warp::body::json())
    .and(with_raft(raft.clone()))
    .and_then(|req, raft: Arc<ChronosRaft>| async move {
        let res = raft.install_snapshot(req).await;
        Ok::<_, warp::Rejection>(warp::reply::json(&res.unwrap()))
    });

    // --- ADMIN ROUTES ---

    // 4. POST /init (Bootstrap Cluster)
    let init = warp::post()
    .and(warp::path("init"))
    .and(with_raft(raft.clone()))
    .and_then(|raft: Arc<ChronosRaft>| async move {
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(1, BasicNode { addr: "127.0.0.1:20001".to_string() });
        let res = raft.initialize(nodes).await;
        Ok::<_, warp::Rejection>(warp::reply::json(&res))
    });

    // 5. POST /add-learner
    let add_learner = warp::post()
    .and(warp::path("add-learner"))
    .and(warp::body::json())
    .and(with_raft(raft.clone()))
    .and_then(|(id, addr): (NodeId, String), raft: Arc<ChronosRaft>| async move {
        let node = BasicNode { addr };
        let res = raft.add_learner(id, node, true).await;
        Ok::<_, warp::Rejection>(warp::reply::json(&res))
    });

    // 6. POST /change-membership
    let change_mem = warp::post()
    .and(warp::path("change-membership"))
    .and(warp::body::json())
    .and(with_raft(raft.clone()))
    .and_then(|ids: std::collections::BTreeSet<NodeId>, raft: Arc<ChronosRaft>| async move {
        let res = raft.change_membership(ids, false).await;
        Ok::<_, warp::Rejection>(warp::reply::json(&res))
    });

    // 7. POST /build-snapshot
    // Manually triggers a snapshot, waits for it to complete, and purges old logs.
    // Critical for managing disk usage in production.
    let build_snapshot = warp::post()
    .and(warp::path("build-snapshot"))
    .and(with_raft(raft.clone()))
    .and_then(|raft: Arc<ChronosRaft>| async move {
        println!("--- Manual Snapshot Triggered ---");

        // A. Identify Target Index
        let target_index = {
            let metrics_rx = raft.metrics();
            let metrics = metrics_rx.borrow();
            metrics.last_log_index.unwrap_or(0)
        };

        if target_index == 0 {
            return Ok::<_, warp::Rejection>(warp::reply::json(&true));
        }

        // B. Trigger Snapshot
        if let Err(e) = raft.trigger().snapshot().await {
            println!("Trigger failed: {:?}", e);
            return Ok::<_, warp::Rejection>(warp::reply::json(&false));
        }

        // C. Poll for Completion (Max 30s)
        let mut purge_ready = false;
        for _ in 0..300 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let metrics_rx = raft.metrics();
            let metrics = metrics_rx.borrow();

            if let Some(snap_log_id) = metrics.snapshot {
                if snap_log_id.index >= target_index {
                    purge_ready = true;
                    break;
                }
            }
        }

        // D. Purge Logs
        if purge_ready {
            println!("Purging logs up to {}", target_index);
            let purge_res = raft.trigger().purge_log(target_index).await;
            match purge_res {
                Ok(_) => Ok::<_, warp::Rejection>(warp::reply::json(&true)),
              Err(e) => {
                  println!("Purge failed: {:?}", e);
                  Ok::<_, warp::Rejection>(warp::reply::json(&false))
              }
            }
        } else {
            println!("TIMEOUT: Snapshot did not complete in time.");
            Ok::<_, warp::Rejection>(warp::reply::json(&false))
        }
    });

    let routes = vote.or(append).or(snapshot).or(init).or(add_learner).or(change_mem).or(build_snapshot);
    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
}

fn with_raft(raft: Arc<ChronosRaft>) -> impl Filter<Extract = (Arc<ChronosRaft>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || raft.clone())
}
