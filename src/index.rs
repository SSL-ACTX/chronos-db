use std::collections::{HashMap, BinaryHeap, HashSet};
use std::sync::RwLock;
use std::cmp::Ordering;
use ordered_float::OrderedFloat;
use rand::Rng;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::fs::File;
use std::path::Path;
use crate::vector::Metric;
use crate::model::VECTOR_DIM;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Candidate {
    dist: OrderedFloat<f32>,
    node_id: u128,
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other.dist.cmp(&self.dist)
    }
}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
pub struct Node {
    pub id: u128,
    pub vector: Vec<f32>,
    pub connections: Vec<Vec<u128>>,
}

pub struct HnswIndex {
    nodes: RwLock<HashMap<u128, Node>>,
    entry_point: RwLock<Option<u128>>,
    max_layer: RwLock<usize>,
    m_max: usize,
    _ef_construction: usize,
    metric: Metric,
}

impl HnswIndex {
    pub fn new(m_max: usize, ef: usize) -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            entry_point: RwLock::new(None),
            max_layer: RwLock::new(0),
            m_max,
            _ef_construction: ef,
            metric: Metric::Cosine,
        }
    }

    pub fn clear(&self) {
        let mut nodes = self.nodes.write().unwrap();
        nodes.clear();
        let mut ep = self.entry_point.write().unwrap();
        *ep = None;
        let mut ml = self.max_layer.write().unwrap();
        *ml = 0;
    }

    /// Logical Removal from the Graph.
    /// NOTE: We remove the node from the map but leave incoming connections dangling.
    /// The search logic is robust enough to ignore missing neighbors.
    pub fn remove(&self, id: u128) {
        let mut nodes = self.nodes.write().unwrap();
        nodes.remove(&id);
    }

    pub fn insert(&self, id: u128, vector: Vec<f32>) {
        let layers = self.select_level();
        let mut node = Node {
            id,
            vector: vector.clone(),
            connections: vec![vec![]; layers + 1],
        };

        let mut ep_guard = self.entry_point.write().unwrap();
        let mut max_layer_guard = self.max_layer.write().unwrap();
        let mut nodes_guard = self.nodes.write().unwrap();

        if ep_guard.is_none() {
            *ep_guard = Some(id);
            *max_layer_guard = layers;
            nodes_guard.insert(id, node);
            return;
        }

        let entry_point_id = ep_guard.unwrap();
        let max_layer = *max_layer_guard;
        let mut curr_obj = entry_point_id;

        // Traverse upper layers greedily to find entry point for layer 0
        for lc in (layers + 1..=max_layer).rev() {
            if let Some(curr_node) = nodes_guard.get(&curr_obj) {
                if lc >= curr_node.connections.len() { break; }

                let n_vec = &curr_node.vector;
                let mut min_dist = self.metric.distance(&vector, n_vec);

                let mut changed = true;
                while changed {
                    changed = false;
                    let candidates = nodes_guard[&curr_obj].connections[lc].clone();
                    for neighbor_id in candidates {
                        if let Some(neighbor) = nodes_guard.get(&neighbor_id) {
                            let d = self.metric.distance(&vector, &neighbor.vector);
                            if d < min_dist {
                                min_dist = d;
                                curr_obj = neighbor_id;
                                changed = true;
                            }
                        }
                    }
                }
            } else {
                break;
            }
        }

        let m_max = self.m_max;
        let curr_entry = curr_obj;

        // Insert into all layers from 0 up to `layers`
        for lc in (0..=std::cmp::min(layers, max_layer)).rev() {
            let mut neighbors = Vec::new();
            neighbors.push(curr_entry);

            if lc < node.connections.len() {
                node.connections[lc].push(curr_entry);
                if let Some(peer) = nodes_guard.get_mut(&curr_entry) {
                    if lc < peer.connections.len() {
                        peer.connections[lc].push(id);
                        // Simple heuristic pruning (keep connections capped at M_max)
                        if peer.connections[lc].len() > m_max {
                            peer.connections[lc].pop();
                        }
                    }
                }
            }
        }

        if layers > max_layer {
            *max_layer_guard = layers;
            *ep_guard = Some(id);
        }
        nodes_guard.insert(id, node);
    }

    fn select_level(&self) -> usize {
        let mut rng = rand::thread_rng();
        let _ml = 1.0 / (self.m_max as f64).ln();
        let mut l = 0;
        while rng.gen::<f32>() < 0.5 {
            l += 1;
        }
        l
    }

    pub fn search(&self, query: &[f32], k: usize) -> Vec<(u128, f32)> {
        let nodes = self.nodes.read().unwrap();
        let ep = self.entry_point.read().unwrap();

        if ep.is_none() {
            return vec![];
        }

        let mut curr_entry = ep.unwrap();

        // Handle case where Entry Point was deleted
        if !nodes.contains_key(&curr_entry) {
            return vec![];
        }

        let max_layer = *self.max_layer.read().unwrap();

        // 1. Zoom in from top layer
        for lc in (1..=max_layer).rev() {
            let mut changed = true;
            if let Some(node) = nodes.get(&curr_entry) {
                if lc >= node.connections.len() { continue; }
                let mut min_dist = self.metric.distance(query, &node.vector);

                while changed {
                    changed = false;
                    if let Some(inner_node) = nodes.get(&curr_entry) {
                        if lc < inner_node.connections.len() {
                            for neighbor in &inner_node.connections[lc] {
                                if let Some(n_node) = nodes.get(neighbor) {
                                    let d = self.metric.distance(query, &n_node.vector);
                                    if d < min_dist {
                                        min_dist = d;
                                        curr_entry = *neighbor;
                                        changed = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // 2. Perform detailed search on Layer 0 (Base Layer)
        let mut candidates = BinaryHeap::new();
        let mut visited = HashSet::new();

        if let Some(node) = nodes.get(&curr_entry) {
            let d = self.metric.distance(query, &node.vector);
            candidates.push(Candidate { dist: OrderedFloat(d), node_id: curr_entry });
            visited.insert(curr_entry);

            let mut results = BinaryHeap::new();
            results.push(Candidate { dist: OrderedFloat(d), node_id: curr_entry });

            while let Some(cand) = candidates.pop() {
                let u = cand.node_id;
                if let Some(node) = nodes.get(&u) {
                    if !node.connections.is_empty() {
                        for v in &node.connections[0] {
                            if !visited.contains(v) {
                                visited.insert(*v);
                                if let Some(target) = nodes.get(v) {
                                    let dist = self.metric.distance(query, &target.vector);
                                    candidates.push(Candidate { dist: OrderedFloat(dist), node_id: *v });
                                    results.push(Candidate { dist: OrderedFloat(dist), node_id: *v });
                                }
                            }
                        }
                    }
                }
            }

            let mut final_res = Vec::new();
            while let Some(c) = results.pop() {
                final_res.push((c.node_id, c.dist.into_inner()));
            }
            final_res.reverse();
            final_res.into_iter().take(k).collect()
        } else {
            vec![]
        }
    }

    pub fn save(&self, path: &Path) -> io::Result<()> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        let nodes = self.nodes.read().unwrap();

        writer.write_all(&(nodes.len() as u32).to_le_bytes())?;

        for (_, node) in nodes.iter() {
            writer.write_all(&node.id.to_le_bytes())?;
            for val in &node.vector {
                writer.write_all(&val.to_le_bytes())?;
            }
            writer.write_all(&(node.connections.len() as u8).to_le_bytes())?;
            for layer in &node.connections {
                writer.write_all(&(layer.len() as u32).to_le_bytes())?;
                for conn in layer {
                    writer.write_all(&conn.to_le_bytes())?;
                }
            }
        }
        Ok(())
    }

    pub fn load(path: &Path, m_max: usize, ef: usize) -> io::Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut count_buf = [0u8; 4];
        reader.read_exact(&mut count_buf)?;
        let count = u32::from_le_bytes(count_buf);

        let mut nodes = HashMap::new();
        let mut max_l = 0;
        let mut ep = None;

        for _ in 0..count {
            let mut id_buf = [0u8; 16];
            reader.read_exact(&mut id_buf)?;
            let id = u128::from_le_bytes(id_buf);

            let mut vector = Vec::with_capacity(VECTOR_DIM);
            let mut f32_buf = [0u8; 4];
            for _ in 0..VECTOR_DIM {
                reader.read_exact(&mut f32_buf)?;
                vector.push(f32::from_le_bytes(f32_buf));
            }

            let mut layers_byte = [0u8; 1];
            reader.read_exact(&mut layers_byte)?;
            let num_layers = layers_byte[0] as usize;

            if num_layers > 0 {
                let level_index = num_layers.saturating_sub(1);
                if level_index > max_l {
                    max_l = level_index;
                    ep = Some(id);
                }
                if ep.is_none() {
                    ep = Some(id);
                    max_l = level_index;
                }
            }

            let mut connections = Vec::with_capacity(num_layers);
            for _ in 0..num_layers {
                let mut link_count_buf = [0u8; 4];
                reader.read_exact(&mut link_count_buf)?;
                let link_count = u32::from_le_bytes(link_count_buf);

                let mut links = Vec::with_capacity(link_count as usize);
                for _ in 0..link_count {
                    let mut link_buf = [0u8; 16];
                    reader.read_exact(&mut link_buf)?;
                    links.push(u128::from_le_bytes(link_buf));
                }
                connections.push(links);
            }
            let node = Node { id, vector, connections };
            nodes.insert(id, node);
        }

        Ok(Self {
            nodes: RwLock::new(nodes),
           entry_point: RwLock::new(ep),
           max_layer: RwLock::new(max_l),
           m_max,
           _ef_construction: ef,
           metric: Metric::Cosine,
        })
    }
}
