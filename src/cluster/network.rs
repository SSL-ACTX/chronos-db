use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError,
};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{
    BasicNode, RaftNetwork, RaftNetworkFactory, RaftTypeConfig,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use crate::cluster::types::{TypeConfig, NodeId};
use std::fmt::{Display, Formatter};

pub struct ChronosNetwork;

impl ChronosNetwork {
    pub fn new() -> Self {
        Self
    }
}

impl RaftNetworkFactory<TypeConfig> for ChronosNetwork {
    type Network = ChronosNetworkConnection;

    async fn new_client(
        &mut self,
        _target: <TypeConfig as RaftTypeConfig>::NodeId,
        node: &<TypeConfig as RaftTypeConfig>::Node,
    ) -> Self::Network {
        ChronosNetworkConnection {
            addr: node.addr.clone(),
            _target,
        }
    }
}

pub struct ChronosNetworkConnection {
    addr: String,
    _target: NodeId,
}

impl ChronosNetworkConnection {
    async fn send_post<Req, Resp, E>(
        &self,
        route: &str,
        req: Req,
    ) -> Result<Resp, RPCError<NodeId, BasicNode, RaftError<NodeId, E>>>
    where
    Req: Serialize,
    Resp: DeserializeOwned,
    E: std::error::Error + 'static,
    {
        let url = format!("http://{}/{}", self.addr, route);
        let client = reqwest::Client::new();

        let resp = client
        .post(&url)
        .json(&req)
        .send()
        .await
        .map_err(|e| {
            RPCError::Network(
                NetworkError::new(&AnyError(e.to_string())),
            )
        })?;

        if !resp.status().is_success() {
            return Err(RPCError::Network(NetworkError::new(
                &AnyError(format!("HTTP error: {}", resp.status())),
            )));
        }

        let res = resp.json::<Resp>().await.map_err(|e| {
            RPCError::Network(
                NetworkError::new(&AnyError(e.to_string())),
            )
        })?;

        Ok(res)
    }
}

#[derive(Debug)]
struct AnyError(String);

impl Display for AnyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for AnyError {}

impl RaftNetwork<TypeConfig> for ChronosNetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
    AppendEntriesResponse<NodeId>,
    RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        self.send_post::<_, _, openraft::error::Infallible>(
            "raft-append",
            req,
        )
        .await
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
    InstallSnapshotResponse<NodeId>,
    RPCError<
    NodeId,
    BasicNode,
    RaftError<NodeId, InstallSnapshotError>,
    >,
    > {
        self.send_post("raft-snapshot", req).await
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<
    VoteResponse<NodeId>,
    RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        self.send_post::<_, _, openraft::error::Infallible>(
            "raft-vote",
            req,
        )
        .await
    }
}
