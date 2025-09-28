use std::{
    collections::HashSet,
    hash::Hash,
    time::{Duration, Instant},
};

use crate::fs::*;
use dashmap::DashMap;
use uuid::Uuid;

pub mod fs;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct INodeId(pub Uuid);

/// A unique identifier for a data block (e.g., a 64MB chunk of a file).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockId(pub u64);

/// A unique identifier for a registered slave/chunkserver node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SlaveId(pub u64);

/// Metadata about a single data block, tracking its location and state.
#[derive(Debug, Clone)]
pub struct BlockInfo {
    /// The list of SlaveIds that currently hold a replica of this block.
    pub locations: Vec<SlaveId>,
    /// The size of the block in bytes. The last block may not be full.
    pub size: u64,
}

/// The health status of a slave node, as determined by the master.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlaveStatus {
    Live,
    Stale,
    Dead,
}

/// Metadata about a registered slave node in the cluster.
#[derive(Debug, Clone)]
pub struct SlaveInfo {
    /// The unique ID assigned to this slave by the master upon registration.
    pub id: SlaveId,
    pub advertise_addr: String,
    /// The last time the master received a heartbeat from this slave.
    pub last_heartbeat: Instant,
    pub status: SlaveStatus,

    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub blocks: HashSet<BlockId>,
}

#[derive(Debug, Clone)]
pub struct MasterConfig {
    pub default_replication_factor: u8,
    pub block_size: u64,
    pub heartbeat_interval: Duration,
    pub stale_timeout: Duration,
    pub dead_timeout: Duration,
    pub safe_mode_threshold: f32,
}

// This struct aggregates all the state required for the master to run.
// It is designed to be wrapped in an `Arc` and shared across threads.
#[derive(Debug)]
pub struct MasterState {
    pub fs: FileSystem,

    /// Maps every `BlockId` to its metadata, including its locations.
    pub block_map: DashMap<BlockId, BlockInfo>,

    /// Maps every `SlaveId` to its metadata, tracking liveness and capacity.
    pub slaves: DashMap<SlaveId, SlaveInfo>,

    pub config: MasterConfig,

    pub next_block_id: std::sync::atomic::AtomicU64,
    pub next_slave_id: std::sync::atomic::AtomicU64,
}

impl MasterState {
    /// Creates a new MasterState.
    pub async fn new(config: MasterConfig) -> Self {
        Self {
            fs: Default::default(),
            block_map: DashMap::new(),
            slaves: DashMap::new(),
            config,
            next_block_id: std::sync::atomic::AtomicU64::new(0),
            next_slave_id: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn insert_block(&mut self, block_info: BlockInfo) {
        let id = self
            .next_block_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.block_map.insert(BlockId(id), block_info);
    }

    pub fn insert_slave(&mut self, slave_info: SlaveInfo) {
        let id = self
            .next_slave_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.slaves.insert(SlaveId(id), slave_info);
    }
}
