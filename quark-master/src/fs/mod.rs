use crate::{BlockId, INodeId};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::time::SystemTime;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{debug, info, trace};
use uuid::Uuid;

type Result<T> = std::result::Result<T, FileError>;

#[derive(Debug)]
pub struct FileSystem {
    /// Maps every `INodeId` to its corresponding `INode` data.
    pub namespace: RwLock<HashMap<INodeId, INode>>,

    pub root_id: INodeId,
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
    /// Creates a new INode with the specified id, parent and specied file / directory data.
    pub fn new(name: String, id: Uuid, parent_id: Uuid, kind: INodeKind) -> Self {
        let now = SystemTime::now();
        Self {
            id: crate::INodeId(id),
            parent: crate::INodeId(parent_id),
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
    pub async fn create_directory(&mut self, path: String) -> Result<()> {
        self.create_inode(path, CreateType::Directory).await
    }

    pub async fn create_file(&mut self, path: String) -> Result<()> {
        self.create_inode(path, CreateType::File).await
    }

    async fn create_inode(&mut self, path: String, create_type: CreateType) -> Result<()> {
        match create_type {
            CreateType::Directory if !path.ends_with("/") => {
                return Err(FileError::DirectoryError);
            }
            CreateType::File if path.ends_with("/") => {
                return Err(FileError::FileError);
            }
            _ => {}
        }

        let path_obj = Path::new(&path);
        if !path_obj.is_absolute() {
            return Err(FileError::NonAbsolutePath);
        }

        let parent = path_obj.parent().ok_or(FileError::RootForbidden)?;
        let name = path_obj
            .file_name()
            .ok_or(FileError::FileNotFound)?
            .to_str()
            .ok_or(FileError::FileNotFound)?
            .to_string();

        let parent_id = self.walk_inode_tree(parent).await?;

        let new_inode = match create_type {
            CreateType::Directory => INode::create_directory(name.clone(), parent_id.0),
            CreateType::File => INode::create_file(name.clone(), parent_id.0),
        };

        let new_inode_id = new_inode.id;

        let mut guard = self.namespace.write().await;
        let parent_inode = guard
            .get_mut(&parent_id)
            .ok_or(FileError::PossibleCorruption)?;

        match &mut parent_inode.kind {
            INodeKind::File { .. } => return Err(FileError::FileAsParent),
            INodeKind::Directory { children } => {
                if children.contains_key(&name) {
                    return Err(FileError::AlreadyExists);
                }
                children.insert(name, new_inode_id);
            }
        }

        guard.insert(new_inode_id, new_inode);
        Ok(())
    }
    pub async fn get_inode(&self, path: String) -> Result<INode> {
        let path = Path::new(&path);
        let inode_id = self.walk_inode_tree(path).await?;
        let guard = self.namespace.read().await;
        let inode = guard.get(&inode_id).ok_or(FileError::PossibleCorruption)?;
        Ok(inode.clone())
    }

    /// Find the id of the INode for the given path
    async fn walk_inode_tree(&self, path: &Path) -> Result<INodeId> {
        debug!("Started walking inode tree");
        let guard = self.namespace.read().await;
        let mut current = guard.get(&self.root_id).ok_or(TraversalError::RootError)?;
        trace!("Got root");
        info!("{:?}", path.components());
        for component in path.components().skip(1) {
            let name = component.as_os_str().to_str().ok_or(TraversalError::Utf8)?;
            trace!(component = ?name);
            current = match &current.kind {
                INodeKind::Directory { children } => {
                    let child_id = children.get(name).ok_or(FileError::FileNotFound)?;
                    trace!(?child_id, "Found child");

                    guard.get(child_id).ok_or(FileError::FileNotFound)?
                }
                INodeKind::File { .. } => {
                    debug!("Tried to traverse into a file");
                    return Err(TraversalError::WalkThroughFile.into());
                }
            };
            trace!(?current);
        }
        debug!("Finished walking inode tree");
        Ok(current.id)
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum FileError {
    #[error("Non-absolute paths aren't supported")]
    NonAbsolutePath,
    #[error("Tried to create directory with non directory path")]
    DirectoryError,
    #[error("Tried to create file with non file path")]
    FileError,
    #[error("Cannot modify root directory")]
    RootForbidden,
    #[error("File as parent")]
    FileAsParent,
    #[error("File not found")]
    FileNotFound,
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_create_directory_success() {
        let mut fs = FileSystem::default();

        // Should successfully create a directory
        let result = fs.create_directory("/test/".to_string()).await;
        assert!(result.is_ok());

        // Should be able to retrieve the created directory
        let inode = fs.get_inode("/test/".to_string()).await.unwrap();
        assert_eq!(inode.name, "test");
        assert!(matches!(inode.kind, INodeKind::Directory { .. }));
    }

    #[tokio::test]
    async fn test_cannot_get_inode() {
        let fs = FileSystem::default();

        let result = fs.get_inode("/nonexistent".to_string()).await;

        assert!(matches!(result.unwrap_err(), FileError::FileNotFound));
    }

    #[tokio::test]
    async fn test_create_nested_directories() {
        let mut fs = FileSystem::default();

        fs.create_directory("/parent/".to_string()).await.unwrap();

        fs.create_directory("/parent/child1/".to_string())
            .await
            .unwrap();
        fs.create_directory("/parent/child2/".to_string())
            .await
            .unwrap();

        // Verify both children exist
        let child1 = fs.get_inode("/parent/child1/".to_string()).await.unwrap();
        let child2 = fs.get_inode("/parent/child2/".to_string()).await.unwrap();

        assert_eq!(child1.name, "child1");
        assert_eq!(child2.name, "child2");
        assert!(matches!(child1.kind, INodeKind::Directory { .. }));
        assert!(matches!(child2.kind, INodeKind::Directory { .. }));
    }

    #[tokio::test]
    async fn test_create_file_success() {
        let mut fs = FileSystem::default();

        // Create a file
        let result = fs.create_file("/test.txt".to_string()).await;
        assert!(result.is_ok());

        // Should be able to retrieve the created file
        let inode = fs.get_inode("/test.txt".to_string()).await.unwrap();
        assert_eq!(inode.name, "test.txt");
        assert!(matches!(inode.kind, INodeKind::File { .. }));
    }

    #[tokio::test]
    async fn test_create_file_in_directory() {
        let mut fs = FileSystem::default();

        fs.create_directory("/docs/".to_string()).await.unwrap();

        fs.create_file("/docs/readme.txt".to_string())
            .await
            .unwrap();

        let file_inode = fs.get_inode("/docs/readme.txt".to_string()).await.unwrap();
        assert_eq!(file_inode.name, "readme.txt");
        assert!(matches!(file_inode.kind, INodeKind::File { .. }));
    }

    #[tokio::test]
    async fn test_cannot_traverse_through_file() {
        let mut fs = FileSystem::default();

        fs.create_file("/document.txt".to_string()).await.unwrap();

        // Try to create a directory "inside" the file - should fail with FileAsParent
        let result = fs
            .create_directory("/document.txt/subfolder/".to_string())
            .await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), FileError::FileAsParent));

        // Try to create a directory by traversing through the file - should fail with TraversalError
        let result = fs
            .create_directory("/document.txt/subfolder/abc/".to_string())
            .await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FileError::TraversalError(TraversalError::WalkThroughFile)
        ));

        // Try to create a file "inside" the file - should also fail with TraversalError
        let result = fs
            .create_file("/document.txt/another.txt/another.txt".to_string())
            .await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FileError::TraversalError(TraversalError::WalkThroughFile)
        ));
    }

    #[tokio::test]
    async fn test_path_validation() {
        let mut fs = FileSystem::default();

        // Non-absolute paths should fail
        let result = fs.create_directory("relative/path/".to_string()).await;
        assert!(matches!(result.unwrap_err(), FileError::NonAbsolutePath));

        // Directory paths must end with /
        let result = fs.create_directory("/no-trailing-slash".to_string()).await;
        assert!(matches!(result.unwrap_err(), FileError::DirectoryError));

        // File paths must NOT end with /
        let result = fs.create_file("/file-with-slash/".to_string()).await;
        assert!(matches!(result.unwrap_err(), FileError::FileError));
    }

    #[tokio::test]
    async fn test_parent_must_exist() {
        let mut fs = FileSystem::default();

        let result = fs.create_directory("/nonexistent/child/".to_string()).await;
        assert!(result.is_err());

        let result = fs.create_file("/nonexistent/file.txt".to_string()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cannot_create_duplicate() {
        let mut fs = FileSystem::default();

        // Create directory
        fs.create_directory("/test/".to_string()).await.unwrap();

        // Try to create same directory again - should fail
        let result = fs.create_directory("/test/".to_string()).await;
        assert!(matches!(result.unwrap_err(), FileError::AlreadyExists));

        // Try to create file with same name - should also fail
        let result = fs.create_file("/test".to_string()).await;
        assert!(matches!(result.unwrap_err(), FileError::AlreadyExists));
    }

    #[tokio::test]
    async fn test_complex_filesystem_structure() {
        let mut fs = FileSystem::default();

        fs.create_directory("/abcd/".to_string()).await.unwrap();
        fs.create_directory("/abcd/a/".to_string()).await.unwrap();
        fs.create_directory("/abcd/b/".to_string()).await.unwrap();
        fs.create_file("/abcd.exe".to_string()).await.unwrap();

        let abcd_dir = fs.get_inode("/abcd/".to_string()).await.unwrap();
        let a_dir = fs.get_inode("/abcd/a/".to_string()).await.unwrap();
        let b_dir = fs.get_inode("/abcd/b/".to_string()).await.unwrap();
        let exe_file = fs.get_inode("/abcd.exe".to_string()).await.unwrap();

        assert!(matches!(abcd_dir.kind, INodeKind::Directory { .. }));
        assert!(matches!(a_dir.kind, INodeKind::Directory { .. }));
        assert!(matches!(b_dir.kind, INodeKind::Directory { .. }));
        assert!(matches!(exe_file.kind, INodeKind::File { .. }));

        // Test error cases from original
        let result = fs.create_directory("/abcd.exe/b/".to_string()).await;
        println!("{result:?}");
        assert!(matches!(result.unwrap_err(), FileError::FileAsParent));

        let result = fs.create_file("/abcd.exe/b.exe".to_string()).await;
        assert!(matches!(result.unwrap_err(), FileError::FileAsParent));
    }

    async fn setup_basic_fs() -> FileSystem {
        let mut fs = FileSystem::default();
        fs.create_directory("/tmp/".to_string()).await.unwrap();
        fs.create_directory("/home/".to_string()).await.unwrap();
        fs.create_directory("/etc/".to_string()).await.unwrap();
        fs.create_file("/etc/config".to_string()).await.unwrap();
        fs
    }

    #[tokio::test]
    async fn test_with_setup_helper() {
        let fs = setup_basic_fs().await;

        let tmp_dir = fs.get_inode("/tmp/".to_string()).await.unwrap();
        let config_file = fs.get_inode("/etc/config".to_string()).await.unwrap();

        assert_eq!(tmp_dir.name, "tmp");
        assert_eq!(config_file.name, "config");
    }
}
