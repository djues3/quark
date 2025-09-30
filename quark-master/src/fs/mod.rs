use crate::{BlockId, INodeId};
use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::path::Path;
use std::time::SystemTime;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::debug;
use uuid::Uuid;

type Result<T> = std::result::Result<T, FileError>;

#[derive(Debug)]
pub struct FileSystem {
    /// Maps every `INodeId` to its corresponding `INode` data.
    pub namespace: RwLock<HashMap<INodeId, INode>>,

    pub root_id: INodeId,
}

#[derive(Debug, Clone)]
pub struct DirectoryEntry {
    pub name: String,
    pub id: INodeId,
    pub created_at: SystemTime,
    pub modified_at: SystemTime,
    pub entry_type: EntryType,
}

#[derive(Debug, Clone)]
pub enum EntryType {
    File { block_count: usize },
    Directory { child_count: usize },
}

pub enum FsOp {
    CreateDirectory { path: String },
    CreateFile { path: String },
    DeleteFile { path: String },
    Rename { from: String, to: String },
}

/// Represents a single entry in the filesystem, either a file or a directory.
#[derive(Debug, Clone)]
pub struct INode {
    /// The unique ID of this INode.
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
}

impl Default for FileSystem {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSystem {
    /// Creates a new filesystem with a root directory.
    pub fn new() -> Self {
        let root = INode::create_root();
        let root_id = root.id;
        let mut namespace = HashMap::new();
        namespace.insert(root_id, root);

        Self {
            namespace: RwLock::new(namespace),
            root_id,
        }
    }
    /// Creates a new directory at the specified path.
    /// The path must end with a `/`
    /// The creation will fail if any part of the path doesn't exist.
    pub async fn create_directory(&mut self, path: impl AsRef<Path>) -> Result<()> {
        self.create_inode(path, CreateType::Directory).await
    }

    /// Creates a new file at the specified path.
    /// The path must not end with a `/`
    /// The creation will fail if the parent directory doesn't exist
    pub async fn create_file(&mut self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let path_str = path.to_str().ok_or(TraversalError::Utf8)?;
        if path_str.ends_with("/") {
            return Err(FileError::FileError);
        }

        self.create_inode(path, CreateType::File).await
    }

    async fn create_inode(
        &mut self,
        path: impl AsRef<Path>,
        create_type: CreateType,
    ) -> Result<()> {
        let path = path.as_ref();
        match create_type {
            CreateType::File if path.ends_with("/") => {
                return Err(FileError::FileError);
            }
            _ => {}
        }
        if !path.is_absolute() {
            return Err(FileError::NonAbsolutePath);
        }

        let parent = path.parent().ok_or(FileError::RootForbidden)?;
        let name = path
            .file_name()
            .ok_or(FileError::NotFound)?
            .to_str()
            .ok_or(FileError::NotFound)?
            .to_string();

        let mut guard = self.namespace.write().await;

        let parent_id = self.walk_inode_tree_guarded(&guard, parent)?;

        let parent_inode = guard
            .get_mut(&parent_id)
            .ok_or(FileError::PossibleCorruption)?;

        match &mut parent_inode.kind {
            INodeKind::File { .. } => return Err(FileError::FileAsParent),
            INodeKind::Directory { children } => {
                if children.contains_key(&name) {
                    return Err(FileError::AlreadyExists);
                }

                let new_inode = match create_type {
                    CreateType::Directory => INode::create_directory(name.clone(), parent_id.0),
                    CreateType::File => INode::create_file(name.clone(), parent_id.0),
                };

                let new_inode_id = new_inode.id;

                children.insert(name, new_inode_id);
                guard.insert(new_inode_id, new_inode);
            }
        }

        Ok(())
    }
    pub async fn get_inode(&self, path: impl AsRef<Path>) -> Result<INode> {
        let inode_id = self.walk_inode_tree(path.as_ref()).await?;
        let guard = self.namespace.read().await;
        let inode = guard.get(&inode_id).ok_or(FileError::PossibleCorruption)?;
        Ok(inode.clone())
    }

    /// Returns a `ls` of the specified directory
    pub async fn list_directory(&self, path: impl AsRef<Path>) -> Result<Vec<DirectoryEntry>> {
        //debug!(path = ?path, "Listing directory");
        //let path = Path::new(&path);
        let inode_id = self.walk_inode_tree(path.as_ref()).await?;
        let guard = self.namespace.read().await;
        let inode = guard.get(&inode_id).ok_or(FileError::PossibleCorruption)?;
        match &inode.kind {
            INodeKind::File { .. } => Err(FileError::DirectoryError),
            INodeKind::Directory { children } => {
                let mut entries = children
                    .iter()
                    .map(|(name, &child_id)| {
                        guard
                            .get(&child_id)
                            .ok_or(FileError::PossibleCorruption)
                            .map(|inode| DirectoryEntry::from_inode(name.clone(), inode))
                    })
                    .collect::<Result<Vec<_>>>()?;
                entries.sort_unstable_by(|a, b| {
                    use std::cmp::Ordering;
                    match (&a.entry_type, &b.entry_type) {
                        (EntryType::File { .. }, EntryType::Directory { .. }) => Ordering::Greater,
                        (EntryType::Directory { .. }, EntryType::File { .. }) => Ordering::Less,
                        _ => {
                            let case_insensitive =
                                a.name.to_lowercase().cmp(&b.name.to_lowercase());

                            if case_insensitive == Ordering::Equal {
                                // we want lowercase before uppercase, UTF-8 doesn't agree
                                a.name.cmp(&b.name).reverse()
                            } else {
                                case_insensitive
                            }
                        }
                    }
                });
                Ok(entries)
            }
        }
    }

    /// Find the id of the INode for the given path
    async fn walk_inode_tree(&self, path: impl AsRef<Path>) -> Result<INodeId> {
        debug!("Acquiring read lock to walk inode tree");
        let guard = self.namespace.read().await;

        self.walk_inode_tree_guarded(&guard, path)
    }

    /// Walks INode tree with the provided guard, synchronously
    /// The `G` type must be `tokio::sync::RwLocK{x}Guard`, callers
    /// are expected to uphold this.
    fn walk_inode_tree_guarded<G>(&self, guard: &G, path: impl AsRef<Path>) -> Result<INodeId>
    where
        G: Deref<Target = HashMap<INodeId, INode>>,
    {
        let namespace = &**guard;

        let path = path.as_ref();
        if path == Path::new("/") {
            return Ok(self.root_id);
        }

        let mut current = namespace
            .get(&self.root_id)
            .ok_or(TraversalError::RootError)?;

        for component in path.components().skip(1) {
            let name = component.as_os_str().to_str().ok_or(TraversalError::Utf8)?;

            current = match &current.kind {
                INodeKind::File { .. } => return Err(TraversalError::WalkThroughFile.into()),
                INodeKind::Directory { children } => {
                    let child_id = children.get(name).ok_or(FileError::NotFound)?;

                    namespace
                        .get(child_id)
                        .ok_or(FileError::PossibleCorruption)?
                }
            }
        }

        Ok(current.id)
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum FileError {
    #[error("Non-absolute paths aren't supported")]
    NonAbsolutePath,
    #[error("Tried to create directory with non directory path")]
    DirectoryError,
    #[error("Tried to create file with a directory-like path")]
    FileError,
    #[error("Cannot modify root directory")]
    RootForbidden,
    #[error("File as parent")]
    FileAsParent,
    #[error("File not found")]
    NotFound,
    #[error("File system probably corrupted, file was directory now is file")]
    PossibleCorruption,
    #[error("Error while traversing file system: {0}")]
    TraversalError(#[from] TraversalError),
    #[error("Specified name already exists")]
    AlreadyExists,
}

#[derive(Debug, Error, PartialEq)]
pub enum TraversalError {
    #[error("Tried to walk through File INode")]
    WalkThroughFile,
    #[error("Couldn't find root, filesystem is probably corrupted")]
    RootError,
    #[error("Error parsing UTF-8")]
    Utf8,
}

enum CreateType {
    Directory,
    File,
}

impl DirectoryEntry {
    fn from_inode(name: String, inode: &INode) -> Self {
        Self {
            name,
            id: inode.id,
            created_at: inode.created_at,
            modified_at: inode.modified_at,
            entry_type: match &inode.kind {
                INodeKind::File { blocks } => EntryType::File {
                    block_count: blocks.len(),
                },
                INodeKind::Directory { children } => EntryType::Directory {
                    child_count: children.len(),
                },
            },
        }
    }
}
