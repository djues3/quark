use crate::{BlockId, INodeId};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::time::SystemTime;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{debug, trace};
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
        let path = path.as_ref();
        debug!("Started walking inode tree");
        let guard = self.namespace.read().await;
        let mut current = guard.get(&self.root_id).ok_or(TraversalError::RootError)?;
        trace!("Got root");
        for component in path.components().skip(1) {
            let name = component.as_os_str().to_str().ok_or(TraversalError::Utf8)?;
            trace!(component = ?name);
            current = match &current.kind {
                INodeKind::Directory { children } => {
                    let child_id = children.get(name).ok_or(FileError::NotFound)?;
                    trace!(?child_id, "Found child");

                    guard.get(child_id).ok_or(FileError::NotFound)?
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_create_directory_success() {
        let mut fs = FileSystem::default();

        // Should successfully create a directory
        let result = fs.create_directory("/test/").await;
        assert!(result.is_ok());

        // Should be able to retrieve the created directory
        let inode = fs.get_inode("/test/").await.unwrap();
        assert_eq!(inode.name, "test");
        assert!(matches!(inode.kind, INodeKind::Directory { .. }));
    }

    #[tokio::test]
    async fn test_cannot_get_inode() {
        let fs = FileSystem::default();

        let result = fs.get_inode("/nonexistent").await;

        assert!(matches!(result.unwrap_err(), FileError::NotFound));
    }

    #[tokio::test]
    async fn test_create_nested_directories() {
        let mut fs = FileSystem::default();

        fs.create_directory("/parent/").await.unwrap();

        fs.create_directory("/parent/child1/").await.unwrap();
        fs.create_directory("/parent/child2/").await.unwrap();

        // Verify both children exist
        let child1 = fs.get_inode("/parent/child1/").await.unwrap();
        let child2 = fs.get_inode("/parent/child2/").await.unwrap();

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
        let inode = fs.get_inode("/test.txt").await.unwrap();
        assert_eq!(inode.name, "test.txt");
        assert!(matches!(inode.kind, INodeKind::File { .. }));
    }

    #[tokio::test]
    async fn test_create_file_in_directory() {
        let mut fs = FileSystem::default();

        fs.create_directory("/docs/").await.unwrap();

        fs.create_file("/docs/readme.txt").await.unwrap();

        let file_inode = fs.get_inode("/docs/readme.txt").await.unwrap();
        assert_eq!(file_inode.name, "readme.txt");
        assert!(matches!(file_inode.kind, INodeKind::File { .. }));
    }

    #[tokio::test]
    async fn test_cannot_traverse_through_file() {
        let mut fs = FileSystem::default();

        fs.create_file("/document.txt").await.unwrap();

        // Try to create a directory "inside" the file - should fail with FileAsParent
        let result = fs.create_directory("/document.txt/subfolder/").await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), FileError::FileAsParent));

        // Try to create a directory by traversing through the file - should fail with TraversalError
        let result = fs.create_directory("/document.txt/subfolder/abc/").await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FileError::TraversalError(TraversalError::WalkThroughFile)
        ));

        // Try to create a file "inside" the file - should also fail with TraversalError
        let result = fs
            .create_file("/document.txt/another.txt/another.txt")
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
        let result = fs.create_directory("relative/path/").await;
        assert!(matches!(result.unwrap_err(), FileError::NonAbsolutePath));

        // Directory paths must end with /
        let result = fs.create_directory("/no-trailing-slash").await;
        assert!(result.is_ok());

        // File paths must NOT end with /
        let result = fs.create_file("/file-with-slash/").await;
        assert!(matches!(result.unwrap_err(), FileError::FileError));
    }

    #[tokio::test]
    async fn test_parent_must_exist() {
        let mut fs = FileSystem::default();

        let result = fs.create_directory("/nonexistent/child/").await;
        assert!(result.is_err());

        let result = fs.create_file("/nonexistent/file.txt").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cannot_create_duplicate() {
        let mut fs = FileSystem::default();

        // Create directory
        fs.create_directory("/test/".to_string()).await.unwrap();

        // Try to create same directory again - should fail
        let result = fs.create_directory("/test/").await;
        assert!(matches!(result.unwrap_err(), FileError::AlreadyExists));

        // Try to create file with same name - should also fail
        let result = fs.create_file("/test").await;
        assert!(matches!(result.unwrap_err(), FileError::AlreadyExists));
    }

    #[tokio::test]
    async fn test_complex_filesystem_structure() {
        let mut fs = FileSystem::default();

        fs.create_directory("/abcd/").await.unwrap();
        fs.create_directory("/abcd/a/").await.unwrap();
        fs.create_directory("/abcd/b/").await.unwrap();
        fs.create_file("/abcd.exe").await.unwrap();

        let abcd_dir = fs.get_inode("/abcd/").await.unwrap();
        let a_dir = fs.get_inode("/abcd/a/").await.unwrap();
        let b_dir = fs.get_inode("/abcd/b/").await.unwrap();
        let exe_file = fs.get_inode("/abcd.exe").await.unwrap();

        assert!(matches!(abcd_dir.kind, INodeKind::Directory { .. }));
        assert!(matches!(a_dir.kind, INodeKind::Directory { .. }));
        assert!(matches!(b_dir.kind, INodeKind::Directory { .. }));
        assert!(matches!(exe_file.kind, INodeKind::File { .. }));

        // Test error cases from original
        let result = fs.create_directory("/abcd.exe/b/").await;
        println!("{result:?}");
        assert!(matches!(result.unwrap_err(), FileError::FileAsParent));

        let result = fs.create_file("/abcd.exe/b.exe").await;
        assert!(matches!(result.unwrap_err(), FileError::FileAsParent));
    }

    async fn setup_basic_fs() -> FileSystem {
        let mut fs = FileSystem::default();
        fs.create_directory("/tmp/").await.unwrap();
        fs.create_directory("/home/").await.unwrap();
        fs.create_directory("/etc/").await.unwrap();
        fs.create_file("/etc/config").await.unwrap();
        fs
    }

    #[tokio::test]
    async fn test_with_setup_helper() {
        let fs = setup_basic_fs().await;

        let tmp_dir = fs.get_inode("/tmp/").await.unwrap();
        let config_file = fs.get_inode("/etc/config").await.unwrap();

        assert_eq!(tmp_dir.name, "tmp");
        assert_eq!(config_file.name, "config");
    }

    #[tokio::test]
    async fn test_list_directory_with_mixed_content() {
        let mut fs = FileSystem::default();

        // Create a directory with files and subdirectories
        fs.create_directory("/projects/").await.unwrap();
        fs.create_file("/projects/readme.txt").await.unwrap();
        fs.create_file("/projects/config.json").await.unwrap();
        fs.create_directory("/projects/src/").await.unwrap();
        fs.create_directory("/projects/docs/").await.unwrap();

        // Add some content to subdirectories to test child_count
        fs.create_file("/projects/src/main.rs").await.unwrap();
        fs.create_file("/projects/src/lib.rs").await.unwrap();

        // List the directory
        let entries = fs.list_directory("/projects/").await.unwrap();

        // Should have 4 entries
        assert_eq!(entries.len(), 4);

        // Convert to a map for easier testing
        let entry_map: std::collections::HashMap<String, &DirectoryEntry> =
            entries.iter().map(|e| (e.name.clone(), e)).collect();

        // Verify files
        let readme = entry_map.get("readme.txt").unwrap();
        assert_eq!(readme.name, "readme.txt");
        assert!(matches!(
            readme.entry_type,
            EntryType::File { block_count: 0 }
        ));

        let config = entry_map.get("config.json").unwrap();
        assert_eq!(config.name, "config.json");
        assert!(matches!(
            config.entry_type,
            EntryType::File { block_count: 0 }
        ));

        // Verify directories
        let src_dir = entry_map.get("src").unwrap();
        assert_eq!(src_dir.name, "src");
        assert!(matches!(
            src_dir.entry_type,
            EntryType::Directory { child_count: 2 }
        ));

        let docs_dir = entry_map.get("docs").unwrap();
        assert_eq!(docs_dir.name, "docs");
        assert!(matches!(
            docs_dir.entry_type,
            EntryType::Directory { child_count: 0 }
        ));

        // Verify all entries have valid IDs and timestamps
        for entry in &entries {
            assert_ne!(entry.id.0, uuid::Uuid::nil());
            assert!(entry.created_at <= SystemTime::now());
            assert!(entry.modified_at <= SystemTime::now());
        }
    }

    #[tokio::test]
    async fn test_list_empty_directory() {
        let mut fs = FileSystem::default();
        fs.create_directory("/empty/").await.unwrap();

        let entries = fs.list_directory("/empty/").await.unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[tokio::test]
    async fn test_list_root_directory() {
        let mut fs = FileSystem::default();

        // Add some items to root
        fs.create_directory("/home/").await.unwrap();
        fs.create_directory("/etc/").await.unwrap();
        fs.create_file("/boot.cfg").await.unwrap();

        let entries = fs.list_directory("/").await.unwrap();
        assert_eq!(entries.len(), 3);

        let names: std::collections::HashSet<String> =
            entries.iter().map(|e| e.name.clone()).collect();

        assert!(names.contains("home"));
        assert!(names.contains("etc"));
        assert!(names.contains("boot.cfg"));
    }

    #[tokio::test]
    async fn test_list_directory_error_on_file() {
        let mut fs = FileSystem::default();
        fs.create_file("/not_a_directory.txt").await.unwrap();

        let result = fs.list_directory("/not_a_directory.txt").await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), FileError::DirectoryError));
    }

    #[tokio::test]
    async fn test_list_nonexistent_directory() {
        let fs = FileSystem::default();

        let result = fs.list_directory("/does_not_exist/").await;
        assert!(result.is_err());
        // Should fail during path traversal
    }

    #[tokio::test]
    async fn test_list_directory_entry_types_are_correct() {
        let mut fs = FileSystem::default();

        // Create nested structure to verify child counts
        fs.create_directory("/test/").await.unwrap();
        fs.create_directory("/test/subdir1/").await.unwrap();
        fs.create_directory("/test/subdir2/").await.unwrap();
        fs.create_file("/test/file1.txt").await.unwrap();

        // Add files to subdir1
        fs.create_file("/test/subdir1/nested1.txt").await.unwrap();
        fs.create_file("/test/subdir1/nested2.txt").await.unwrap();
        fs.create_file("/test/subdir1/nested3.txt").await.unwrap();

        let entries = fs.list_directory("/test/").await.unwrap();

        for entry in &entries {
            match &entry.name[..] {
                "subdir1" => {
                    assert!(matches!(
                        entry.entry_type,
                        EntryType::Directory { child_count: 3 }
                    ));
                }
                "subdir2" => {
                    assert!(matches!(
                        entry.entry_type,
                        EntryType::Directory { child_count: 0 }
                    ));
                }
                "file1.txt" => {
                    assert!(matches!(
                        entry.entry_type,
                        EntryType::File { block_count: 0 }
                    ));
                }
                _ => panic!("Unexpected entry: {}", entry.name),
            }
        }
    }

    #[tokio::test]
    async fn test_list_directory_preserves_timestamps() {
        let mut fs = FileSystem::default();

        let before_creation = SystemTime::now();
        fs.create_directory("/timestamped/").await.unwrap();
        fs.create_file("/timestamped/file.txt").await.unwrap();
        let after_creation = SystemTime::now();

        let entries = fs.list_directory("/timestamped/").await.unwrap();
        assert_eq!(entries.len(), 1);

        let file_entry = &entries[0];
        assert!(file_entry.created_at >= before_creation);
        assert!(file_entry.created_at <= after_creation);
        assert!(file_entry.modified_at >= before_creation);
        assert!(file_entry.modified_at <= after_creation);
    }

    #[tokio::test]
    async fn test_list_directory_ids_are_unique() {
        let mut fs = FileSystem::default();

        fs.create_directory("/unique_test/").await.unwrap();
        fs.create_file("/unique_test/file1.txt").await.unwrap();
        fs.create_file("/unique_test/file2.txt").await.unwrap();
        fs.create_directory("/unique_test/subdir/").await.unwrap();

        let entries = fs.list_directory("/unique_test/").await.unwrap();

        let ids: std::collections::HashSet<INodeId> = entries.iter().map(|e| e.id).collect();

        // All IDs should be unique
        assert_eq!(ids.len(), entries.len());

        // No ID should be nil
        for id in &ids {
            assert_ne!(id.0, uuid::Uuid::nil());
        }
    }

    #[tokio::test]
    async fn test_list_directory_with_trailing_slash_variations() {
        let mut fs = FileSystem::default();

        fs.create_directory("/testdir/").await.unwrap();
        fs.create_file("/testdir/file.txt").await.unwrap();

        let entries1 = fs.list_directory("/testdir/").await.unwrap();
        let entries2 = fs.list_directory("/testdir").await.unwrap();

        assert_eq!(entries1.len(), entries2.len());
        assert_eq!(entries1[0].name, entries2[0].name);
        assert_eq!(entries1[0].id, entries2[0].id);
    }
    #[tokio::test]
    async fn test_list_directory_maintains_iteration_order() {
        let mut fs = FileSystem::default();

        // Create initial directory structure in a specific order
        fs.create_directory("/ordered/").await.unwrap();
        fs.create_directory("/ordered/apple/").await.unwrap();
        fs.create_directory("/ordered/banana/").await.unwrap();
        fs.create_directory("/ordered/cherry/").await.unwrap();
        fs.create_directory("/ordered/date/").await.unwrap();
        fs.create_directory("/ordered/elderberry/").await.unwrap();

        // First listing
        let entries_before = fs.list_directory("/ordered/").await.unwrap();
        let names_before: Vec<String> = entries_before.iter().map(|e| e.name.clone()).collect();

        // Should be in alphabetical order due to BTreeMap
        assert_eq!(
            names_before,
            vec!["apple", "banana", "cherry", "date", "elderberry"]
        );

        // Add a new directory that should appear in the middle alphabetically
        fs.create_directory("/ordered/coconut/").await.unwrap();

        // Second listing
        let entries_after = fs.list_directory("/ordered/").await.unwrap();
        let names_after: Vec<String> = entries_after.iter().map(|e| e.name.clone()).collect();

        // New order should include coconut in the right position
        assert_eq!(
            names_after,
            vec![
                "apple",
                "banana",
                "cherry",
                "coconut", // <- New directory inserted here
                "date",
                "elderberry"
            ]
        );

        // Verify that all original entries are still present with same metadata
        let original_entries_map: std::collections::HashMap<String, &DirectoryEntry> =
            entries_before.iter().map(|e| (e.name.clone(), e)).collect();
        let new_entries_map: std::collections::HashMap<String, &DirectoryEntry> =
            entries_after.iter().map(|e| (e.name.clone(), e)).collect();

        for (name, original_entry) in original_entries_map {
            let current_entry = new_entries_map
                .get(&name)
                .unwrap_or_else(|| panic!("Original entry '{}' should still exist", name));

            // Same ID, timestamps, and type
            assert_eq!(original_entry.id, current_entry.id);
            assert_eq!(original_entry.created_at, current_entry.created_at);
            assert_eq!(original_entry.modified_at, current_entry.modified_at);
            assert!(matches!(
                (&original_entry.entry_type, &current_entry.entry_type),
                (EntryType::File { .. }, EntryType::File { .. })
                    | (EntryType::Directory { .. }, EntryType::Directory { .. })
            ));
        }

        // Verify the new entry exists and is correct
        let coconut_entry = new_entries_map.get("coconut").unwrap();
        assert!(matches!(
            coconut_entry.entry_type,
            EntryType::Directory { child_count: 0 }
        ));
    }

    /// Note: correctly is defined as:
    /// 1) directories before files
    /// 2) if equal by 1) then by case-insensitive comparison of names
    /// 3) if equal by 2) then by case-sensitive comparison of names
    #[tokio::test]
    async fn test_list_directory_sorts_correctly() {
        let mut fs = FileSystem::default();

        fs.create_directory("/sorted-1/").await.unwrap();
        fs.create_directory("/sorted-2-files/").await.unwrap();
        fs.create_directory("/sorted-2-dirs/").await.unwrap();
        fs.create_directory("/sorted-3-files/").await.unwrap();
        fs.create_directory("/sorted-3-dirs/").await.unwrap();

        // 1) directories before files

        fs.create_file("/sorted-1/a").await.unwrap();
        fs.create_directory("/sorted-1/b/").await.unwrap();

        let entries = fs.list_directory("/sorted-1").await.unwrap();
        let names: Vec<String> = entries.iter().map(|e| e.name.clone()).collect();
        assert_eq!(names, vec!["b", "a"]);

        // 2) case-insensitive comparison of names

        fs.create_file("/sorted-2-files/a").await.unwrap();
        fs.create_file("/sorted-2-files/B").await.unwrap();

        fs.create_directory("/sorted-2-dirs/c/").await.unwrap();
        fs.create_directory("/sorted-2-dirs/A/").await.unwrap();

        let entries = fs.list_directory("/sorted-2-files").await.unwrap();
        let names: Vec<String> = entries.iter().map(|e| e.name.clone()).collect();
        assert_eq!(names, vec!["a", "B"]);

        let entries = fs.list_directory("/sorted-2-dirs").await.unwrap();
        let names: Vec<String> = entries.iter().map(|e| e.name.clone()).collect();
        assert_eq!(names, vec!["A", "c"]);

        // 3) case-sensitive comparison of names
        //
        fs.create_file("/sorted-3-files/a").await.unwrap();
        fs.create_file("/sorted-3-files/A").await.unwrap();

        fs.create_directory("/sorted-3-dirs/a/").await.unwrap();
        fs.create_directory("/sorted-3-dirs/A/").await.unwrap();

        let entries = fs.list_directory("/sorted-3-files").await.unwrap();
        let names: Vec<String> = entries.iter().map(|e| e.name.clone()).collect();
        assert_eq!(names, vec!["a", "A"]);

        let entries = fs.list_directory("/sorted-3-dirs").await.unwrap();
        let names: Vec<String> = entries.iter().map(|e| e.name.clone()).collect();
        assert_eq!(names, vec!["a", "A"]);
    }
}
