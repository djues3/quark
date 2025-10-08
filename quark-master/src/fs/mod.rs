use crate::{BlockId, INodeId};
use std::collections::HashMap;
use std::path::Path;
use std::time::SystemTime;
use tokio::sync::RwLock;

pub mod errors;
pub mod inode;
mod path;

pub use errors::*;
pub use inode::*;
use path::normalize_path;
#[cfg(test)]
use tokio::sync::RwLockReadGuard;
use tracing::{error, info, trace};
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

enum CreateType {
    Directory,
    File,
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
    /// The creation will fail if any part of the path doesn't exist.
    pub async fn create_directory(&self, path: impl AsRef<Path>) -> Result<()> {
        self.create_inode(path, CreateType::Directory).await
    }

    /// Creates a new file at the specified path.
    /// The path must not end with a `/`
    /// The creation will fail if the parent directory, or any part of the path, doesn't exist
    pub async fn create_file(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();

        // TODO: make this nicer?
        if path
            .to_str()
            .ok_or(PathNormalizationError::InvalidUtf8)?
            .ends_with("/")
        {
            return Err(FileError::FileError);
        }

        let path = normalize_path(path)?;

        self.create_inode(path, CreateType::File).await
    }

    /// Returns the `INode` matching the provided path.
    /// Currently, it clones the INode to get around ownership constraints.
    /// Since `BTreeMap`s can be large, use `stat` unless you need this data
    pub async fn get_inode(&self, path: impl AsRef<Path>) -> Result<INode> {
        let path = normalize_path(path)?;
        let guard = self.namespace.read().await;
        let inode_id = walk_inode_tree(&guard, &self.root_id, path)?;
        let inode = guard.get(&inode_id).ok_or(FileError::PossibleCorruption)?;
        Ok(inode.clone())
    }

    /// Returns a `ls` of the specified directory
    pub async fn list_directory(&self, path: impl AsRef<Path>) -> Result<Vec<DirectoryEntry>> {
        let path = normalize_path(path)?;
        let guard = self.namespace.read().await;
        let inode_id = walk_inode_tree(&guard, &self.root_id, path)?;
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

    /// Returns the `DirectoryEntry` matching the provided path.
    pub async fn stat(&self, path: impl AsRef<Path>) -> Result<DirectoryEntry> {
        let path = normalize_path(path)?;
        let guard = self.namespace.read().await;
        let inode_id = walk_inode_tree(&guard, &self.root_id, path)?;
        let inode = guard.get(&inode_id).ok_or(FileError::PossibleCorruption)?;

        Ok(DirectoryEntry::from_inode(inode.name.clone(), inode))
    }

    async fn create_inode(&self, path: impl AsRef<Path>, create_type: CreateType) -> Result<()> {
        let path = normalize_path(path)?;

        let parent = path.parent().ok_or(FileError::RootForbidden)?;

        let name = path
            .file_name()
            .ok_or(FileError::NotFound)?
            .to_str()
            .ok_or(FileError::NotFound)?
            .to_string();

        let mut guard = self.namespace.write().await;

        let parent_id = walk_inode_tree(&guard, &self.root_id, parent)?;

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

    /// Deletes the requested INode.
    /// Returns the blocks which need to be deleted, if any, or an error.
    pub async fn delete(&self, path: impl AsRef<Path>, recursive: bool) -> Result<Vec<BlockId>> {
        let path = normalize_path(path)?;

        if path == Path::new("/") {
            error!("Tried to delete root");
            return Err(FileError::CannotDeleteRoot);
        }
        info!("Deleting path: {path:?}");

        let mut guard = self.namespace.write().await;
        let inode_id = walk_inode_tree(&guard, &self.root_id, &path)?;
        trace!("Got inode_id: {inode_id:?}");

        // get reference instead of removing from the map since errors could still occur
        // so there is no need to handle rolling back transactions.
        let inode = guard.get(&inode_id).ok_or(FileError::PossibleCorruption)?;

        // DFS stack used to avoid recursion
        let mut stack = Vec::new();

        let mut to_delete = Vec::new();

        stack.push(inode_id);
        let mut block_ids: Vec<BlockId> = Vec::new();
        // The deleting of `INode`s is a two step process:
        // 1) add all `INodeId`s to delete to the visited set (this also tracks all ids to delete)
        // 2) iterate through the visited set and delete all the `INode`s this also makes the code simpler,
        // since there is no need to rollback any deletions on an error
        while let Some(id) = stack.pop() {
            to_delete.push(id);
            trace!("New iterations; stack: {stack:?}, current: {}", id.0);

            let inode = guard.get(&id).ok_or(FileError::PossibleCorruption)?;
            match &inode.kind {
                // `BlockId` implements `Copy` so this will copy the ids
                INodeKind::File { blocks } => block_ids.extend(blocks.iter()),
                INodeKind::Directory { children } => {
                    // Cannot delete a directory without recursive mode (matches Unix)
                    if !recursive {
                        return Err(FileError::NonRecursiveDelete);
                    }
                    stack.extend(children.values())
                }
            }
        }

        let parent_id = inode.parent;
        // `walk_inode_tree` checked that the parent must exist, so unwrap is fine.
        let parent = guard.get_mut(&parent_id).unwrap();

        // actually deletes the `INode`s from the filesystem
        if let INodeKind::Directory { children } = &mut parent.kind {
            // path is guaranteed to contain at least one component due to root being disallowed
            let name = path.components().next_back().unwrap();
            debug_assert!(!name.as_os_str().is_empty());
            children.remove(&name.as_os_str().to_str().unwrap().to_string());
        }
        for id in to_delete {
            guard.remove(&id);
        }

        Ok(block_ids)
    }

    pub async fn allocate_blocks(
        &self,
        path: impl AsRef<Path>,
        blocks: impl IntoIterator<Item = BlockId>,
    ) -> Result<()> {
        let path = normalize_path(path)?;
        let mut guard = self.namespace.write().await;
        let inode_id = walk_inode_tree(&guard, &self.root_id, path)?;
        let inode = guard
            .get_mut(&inode_id)
            .ok_or(FileError::PossibleCorruption)?;
        inode
            .append_blocks(blocks)
            .map_err(|_| FileError::AppendBlocksToDirectory)?;

        Ok(())
    }

    #[cfg(test)]
    pub async fn read(&self) -> RwLockReadGuard<'_, HashMap<INodeId, INode>> {
        self.namespace.read().await
    }
}

/// Walks `INode` tree with the provided map.
/// Assumes that the provided `root_id` matches the `root_id` of the provided namespace
fn walk_inode_tree(
    namespace: &HashMap<INodeId, INode>,
    root_id: &INodeId,
    path: impl AsRef<Path>,
) -> Result<INodeId> {
    let path = path.as_ref();
    if path == Path::new("/") {
        return Ok(*root_id);
    }

    let mut current = namespace.get(root_id).ok_or(TraversalError::RootError)?;

    for component in path.components().skip(1) {
        // path is all valid UTF-8 as checked by `normalize_path`. so unwrap is ok
        let name = component.as_os_str().to_str().unwrap();

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

impl Default for FileSystem {
    fn default() -> Self {
        Self::new()
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_delete_nested_directories() {
        let fs = FileSystem::default();

        fs.create_directory("/a/").await.unwrap();
        fs.create_directory("/a/b/").await.unwrap();
        fs.create_directory("/a/b/c/").await.unwrap();
        fs.create_file("/a/b/c/file.txt").await.unwrap();
        fs.create_file("/a/file1.txt").await.unwrap();

        let id = fs.get_inode("/a/").await.unwrap().id;

        fs.delete("/a/", true).await.unwrap();
        // Verify entire tree is gone
        assert!(fs.get_inode("/a/").await.is_err());
        assert!(fs.get_inode("/a/b/").await.is_err());
        assert!(fs.get_inode("/a/b/c/").await.is_err());
        assert!(fs.get_inode("/a/b/c/file.txt").await.is_err());
        assert!(fs.get_inode("/a/file1.txt").await.is_err());
        let read = fs.read().await;
        let inode = read.get(&id);
        assert!(inode.is_none());
    }

    #[tokio::test]
    async fn test_delete_returns_correct_blocks() {
        let fs = FileSystem::default();

        fs.create_directory("/a/").await.unwrap();
        fs.create_directory("/a/b/").await.unwrap();
        fs.create_directory("/a/b/c/").await.unwrap();
        fs.create_file("/a/b/c/file.txt").await.unwrap();
        fs.create_file("/a/file1.txt").await.unwrap();
        let blocks = [BlockId(1), BlockId(2), BlockId(3)];
        fs.allocate_blocks("/a/file1.txt", blocks).await.unwrap();
        let blocks = [BlockId(4), BlockId(5), BlockId(6)];
        fs.allocate_blocks("/a/b/c/file.txt", blocks).await.unwrap();

        let id = fs.get_inode("/a/").await.unwrap().id;

        let returned_blocks = fs.delete("/a/", true).await.unwrap();
        // verify entire tree is gone
        assert!(fs.get_inode("/a/").await.is_err());
        assert!(fs.get_inode("/a/b/").await.is_err());
        assert!(fs.get_inode("/a/b/c/").await.is_err());
        assert!(fs.get_inode("/a/b/c/file.txt").await.is_err());
        assert!(fs.get_inode("/a/file1.txt").await.is_err());

        // verify all blocks are returned by delete so that they can be deallocated
        let check = [
            BlockId(1),
            BlockId(2),
            BlockId(3),
            BlockId(4),
            BlockId(5),
            BlockId(6),
        ];
        assert!(check.iter().all(|b| returned_blocks.contains(b)));

        // verify deleted `INode` is not in the namsepace
        let read = fs.read().await;
        let inode = read.get(&id);
        assert!(inode.is_none());
    }
}
