use std::time::SystemTime;

use quark_master::{INodeId, fs::*};

#[tokio::test]
async fn test_create_directory_success() {
    let fs = FileSystem::default();

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
    let fs = FileSystem::default();

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
    let fs = FileSystem::default();

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
    let fs = FileSystem::default();

    fs.create_directory("/docs/").await.unwrap();

    fs.create_file("/docs/readme.txt").await.unwrap();

    let file_inode = fs.get_inode("/docs/readme.txt").await.unwrap();
    assert_eq!(file_inode.name, "readme.txt");
    assert!(matches!(file_inode.kind, INodeKind::File { .. }));
}

#[tokio::test]
async fn test_cannot_traverse_through_file() {
    let fs = FileSystem::default();

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
    let fs = FileSystem::default();

    // Non-absolute paths should fail
    let result = fs.create_directory("relative/path/").await;
    assert!(matches!(result.unwrap_err(), FileError::PathError(..)));

    // Directory paths must end with /
    let result = fs.create_directory("/no-trailing-slash").await;
    assert!(result.is_ok());

    // File paths must NOT end with /
    let result = fs.create_file("/file-with-slash/").await;
    assert!(matches!(result.unwrap_err(), FileError::FileError));
}

#[tokio::test]
async fn test_parent_must_exist() {
    let fs = FileSystem::default();

    let result = fs.create_directory("/nonexistent/child/").await;
    assert!(result.is_err());

    let result = fs.create_file("/nonexistent/file.txt").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_cannot_create_duplicate() {
    let fs = FileSystem::default();

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
    let fs = FileSystem::default();

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
    let fs = FileSystem::default();
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
    let fs = FileSystem::default();

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
    let fs = FileSystem::default();
    fs.create_directory("/empty/").await.unwrap();

    let entries = fs.list_directory("/empty/").await.unwrap();
    assert_eq!(entries.len(), 0);
}

#[tokio::test]
async fn test_list_root_directory() {
    let fs = FileSystem::default();

    // Add some items to root
    fs.create_directory("/home/").await.unwrap();
    fs.create_directory("/etc/").await.unwrap();
    fs.create_file("/boot.cfg").await.unwrap();

    let entries = fs.list_directory("/").await.unwrap();
    assert_eq!(entries.len(), 3);

    let names: std::collections::HashSet<String> = entries.iter().map(|e| e.name.clone()).collect();

    assert!(names.contains("home"));
    assert!(names.contains("etc"));
    assert!(names.contains("boot.cfg"));
}

#[tokio::test]
async fn test_list_directory_error_on_file() {
    let fs = FileSystem::default();
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
    let fs = FileSystem::default();

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
    let fs = FileSystem::default();

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
    let fs = FileSystem::default();

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
    let fs = FileSystem::default();

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
    let fs = FileSystem::default();

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
    let fs = FileSystem::default();

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

#[tokio::test]
async fn test_stat_fail() {
    let fs = FileSystem::default();

    fs.create_directory("/testdir/").await.unwrap();
    fs.create_file("/testdir/file.txt").await.unwrap();

    let res = fs.stat("/teGtdir/").await;
    assert!(matches!(res.unwrap_err(), FileError::NotFound));
}

#[tokio::test]
async fn test_stat_success() {
    let fs = FileSystem::default();

    fs.create_directory("/testdir/").await.unwrap();
    fs.create_file("/testdir/file.txt").await.unwrap();

    let res = fs.stat("/testdir").await;
    assert!(matches!(
        res.unwrap().entry_type,
        EntryType::Directory { .. }
    ));

    let res = fs.stat("/testdir/file.txt").await;
    assert!(matches!(res.unwrap().entry_type, EntryType::File { .. }));
}

#[tokio::test]
async fn test_delete_file() {
    let fs = FileSystem::default();

    fs.create_file("/document.txt").await.unwrap();

    let blocks = fs.delete("/document.txt", false).await.unwrap();
    assert!(blocks.is_empty()); // New file has no blocks yet

    // Verify file is gone
    let result = fs.get_inode("/document.txt").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_delete_empty_directory_without_recursive() {
    let fs = FileSystem::default();

    fs.create_directory("/empty/").await.unwrap();

    // Should fail - directories require recursive flag
    let result = fs.delete("/empty/", false).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FileError::NonRecursiveDelete));

    // Verify directory still exists
    let inode = fs.get_inode("/empty/").await.unwrap();
    assert!(matches!(inode.kind, INodeKind::Directory { .. }));
}

#[tokio::test]
async fn test_delete_empty_directory_with_recursive() {
    let fs = FileSystem::default();

    fs.create_directory("/empty/").await.unwrap();

    fs.delete("/empty/", true).await.unwrap();

    // Verify directory is gone
    let result = fs.get_inode("/empty/").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_delete_non_empty_directory_without_recursive() {
    let fs = FileSystem::default();

    fs.create_directory("/docs/").await.unwrap();
    fs.create_file("/docs/readme.txt").await.unwrap();

    // Should fail - non-empty directory without recursive
    let result = fs.delete("/docs/", false).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FileError::NonRecursiveDelete));

    // Verify directory and file still exist
    assert!(fs.get_inode("/docs/").await.is_ok());
    assert!(fs.get_inode("/docs/readme.txt").await.is_ok());
}

#[tokio::test]
async fn test_delete_non_empty_directory_with_recursive() {
    let fs = FileSystem::default();

    fs.create_directory("/docs/").await.unwrap();
    fs.create_file("/docs/readme.txt").await.unwrap();
    fs.create_file("/docs/guide.txt").await.unwrap();

    fs.delete("/docs/", true).await.unwrap();

    // Verify directory and all files are gone
    assert!(fs.get_inode("/docs/").await.is_err());
    assert!(fs.get_inode("/docs/readme.txt").await.is_err());
    assert!(fs.get_inode("/docs/guide.txt").await.is_err());
}

#[tokio::test]
async fn test_delete_nested_directories() {
    let fs = FileSystem::default();

    fs.create_directory("/a/").await.unwrap();
    fs.create_directory("/a/b/").await.unwrap();
    fs.create_directory("/a/b/c/").await.unwrap();
    fs.create_file("/a/b/c/file.txt").await.unwrap();
    fs.create_file("/a/file1.txt").await.unwrap();

    fs.delete("/a/", true).await.unwrap();

    // Verify entire tree is gone
    assert!(fs.get_inode("/a/").await.is_err());
    assert!(fs.get_inode("/a/b/").await.is_err());
    assert!(fs.get_inode("/a/b/c/").await.is_err());
    assert!(fs.get_inode("/a/b/c/file.txt").await.is_err());
    assert!(fs.get_inode("/a/file1.txt").await.is_err());
}

#[tokio::test]
async fn test_delete_root_fails() {
    let fs = FileSystem::default();

    let result = fs.delete("/", true).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FileError::CannotDeleteRoot));
}

#[tokio::test]
async fn test_delete_nonexistent_path() {
    let fs = FileSystem::default();

    let result = fs.delete("/nonexistent.txt", false).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_delete_file_in_nested_directory() {
    let fs = FileSystem::default();

    fs.create_directory("/docs/").await.unwrap();
    fs.create_directory("/docs/guides/").await.unwrap();
    fs.create_file("/docs/guides/setup.txt").await.unwrap();

    fs.delete("/docs/guides/setup.txt", false).await.unwrap();

    // Verify only the file is gone, directories remain
    assert!(fs.get_inode("/docs/").await.is_ok());
    assert!(fs.get_inode("/docs/guides/").await.is_ok());
    assert!(fs.get_inode("/docs/guides/setup.txt").await.is_err());
}

#[tokio::test]
async fn test_delete_preserves_siblings() {
    let fs = FileSystem::default();

    fs.create_directory("/parent/").await.unwrap();
    fs.create_file("/parent/file1.txt").await.unwrap();
    fs.create_file("/parent/file2.txt").await.unwrap();
    fs.create_directory("/parent/subdir/").await.unwrap();

    fs.delete("/parent/file1.txt", false).await.unwrap();

    // Verify siblings still exist
    assert!(fs.get_inode("/parent/").await.is_ok());
    assert!(fs.get_inode("/parent/file2.txt").await.is_ok());
    assert!(fs.get_inode("/parent/subdir/").await.is_ok());
    assert!(fs.get_inode("/parent/file1.txt").await.is_err());
}
