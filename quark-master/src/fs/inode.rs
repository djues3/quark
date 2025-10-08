use std::{collections::BTreeMap, time::SystemTime};

use thiserror::Error;
use uuid::Uuid;

use crate::{BlockId, INodeId};

/// Represents a single entry in the filesystem, either a file or a directory.
#[derive(Debug, Clone)]
pub struct INode {
    /// The unique ID of this `Node.
    pub id: INodeId,
    /// The ID of the parent directory. The root's parent is itself.
    pub parent: INodeId,
    /// The name of this node within its parent directory.
    pub name: String,
    /// The creation timestamp.
    pub created_at: SystemTime,
    /// The last modification timestamp.
    pub modified_at: SystemTime,
    /// The data specific to whether this is a file or a directory.
    pub kind: INodeKind,
}

/// The specific data for either a file or a directory.
#[derive(Debug, Clone)]
pub enum INodeKind {
    /// A file, which is composed of an ordered sequence of blocks.
    File {
        /// The list of block IDs that make up this file's content.
        blocks: Vec<BlockId>,
    },
    /// A directory, which contains other INodes.
    Directory {
        /// A map of child names to their respective INode IDs.
        children: BTreeMap<String, INodeId>,
    },
}

impl INode {
    /// Creates a new INode with the specified id, parent and specified file / directory data.
    pub fn new(name: String, id: Uuid, parent_id: Uuid, kind: INodeKind) -> Self {
        let now = SystemTime::now();
        Self {
            id: INodeId(id),
            parent: INodeId(parent_id),
            name,
            created_at: now,
            modified_at: now,
            kind,
        }
    }

    pub fn create_directory(name: String, parent_id: Uuid) -> Self {
        let id = Uuid::now_v7();
        INode::new(
            name,
            id,
            parent_id,
            INodeKind::Directory {
                children: BTreeMap::new(),
            },
        )
    }
    pub fn create_file(name: String, parent_id: Uuid) -> Self {
        let id = Uuid::now_v7();
        INode::new(name, id, parent_id, INodeKind::File { blocks: Vec::new() })
    }

    pub fn create_root() -> Self {
        let now = SystemTime::now();
        let id = Uuid::new_v7(now.try_into().expect("current time is after UNIX_EPOCH"));
        Self {
            id: INodeId(id),
            parent: crate::INodeId(id),
            name: "/".to_string(),
            created_at: now,
            modified_at: now,
            kind: INodeKind::Directory {
                children: BTreeMap::new(),
            },
        }
    }

    pub fn append_blocks(
        &mut self,
        new_blocks: impl IntoIterator<Item = BlockId>,
    ) -> Result<(), INodeError> {
        match &mut self.kind {
            INodeKind::File { blocks } => {
                blocks.extend(new_blocks);
                Ok(())
            }
            INodeKind::Directory { .. } => Err(INodeError::AppendToDirectory),
        }
    }
}
#[derive(Debug, Error)]
pub enum INodeError {
    #[error("Cannot append blocks to a directory")]
    AppendToDirectory,
}
