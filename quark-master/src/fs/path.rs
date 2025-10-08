//! Contains path utility methods
use super::errors::PathNormalizationError;
use std::path::{Component, Path, PathBuf};

/// Normalizes the path by removing `.`, `..`, and redundant `/`es while making sure that the path:
/// 1) is absolute
/// 2) doesn't contain a Windows path prefix (see ['std::path::Prefix'])
pub fn normalize_path(
    path: impl AsRef<Path>,
) -> std::result::Result<PathBuf, PathNormalizationError> {
    let path = path.as_ref();

    let path_str = path.to_str().ok_or(PathNormalizationError::InvalidUtf8)?;

    if path_str.is_empty() {
        return Err(PathNormalizationError::EmptyPath);
    }

    if !path.is_absolute() {
        return Err(PathNormalizationError::NotAbsolute);
    }

    if path_str.contains('\0') {
        return Err(PathNormalizationError::NullByte);
    }

    if path.components().any(|c| matches!(c, Component::Prefix(_))) {
        return Err(PathNormalizationError::PrefixNotSupported);
    }

    let normalized = path
        .components()
        .fold(PathBuf::new(), |mut acc, component| {
            match component {
                Component::RootDir => acc.push("/"),
                Component::Normal(name) => acc.push(name),
                Component::ParentDir if acc != Path::new("/") => {
                    acc.pop();
                }
                Component::ParentDir | Component::CurDir => { /* ignore */ }
                Component::Prefix(_) => unreachable!("Already checked for prefixes"),
            }
            acc
        });

    Ok(normalized)
}
#[cfg(test)]
mod tests {
    use crate::fs::FileSystem;

    use super::*;

    #[test]
    fn test_normalize_path() {
        // Basic normalization
        assert_eq!(normalize_path("/").unwrap(), PathBuf::from("/"));
        assert_eq!(normalize_path("/home").unwrap(), PathBuf::from("/home"));

        // Remove duplicate slashes
        assert_eq!(
            normalize_path("//home///user//").unwrap(),
            PathBuf::from("/home/user")
        );

        // Handle current directory
        assert_eq!(
            normalize_path("/home/./user").unwrap(),
            PathBuf::from("/home/user")
        );

        // Handle parent directory
        assert_eq!(
            normalize_path("/home/user/../other").unwrap(),
            PathBuf::from("/home/other")
        );

        // Don't go above root
        assert_eq!(normalize_path("/../..").unwrap(), PathBuf::from("/"));
        assert_eq!(
            normalize_path("/home/../../..").unwrap(),
            PathBuf::from("/")
        );

        // Complex case
        assert_eq!(
            normalize_path("/home/user/../other/./file/../dir").unwrap(),
            PathBuf::from("/home/other/dir")
        );

        // Error cases
        assert_eq!(
            normalize_path("relative/path"),
            Err(PathNormalizationError::NotAbsolute)
        );
        assert_eq!(normalize_path(""), Err(PathNormalizationError::EmptyPath));
    }

    #[tokio::test]
    async fn test_filesystem_with_normalization() {
        let fs = FileSystem::new();

        // These should all work and be equivalent
        fs.create_directory("/home").await.unwrap();
        fs.create_directory("//home//user//").await.unwrap();
        fs.create_file("/home/user/../user/file.txt").await.unwrap();

        // Verify the file exists at the normalized path
        let inode = fs.get_inode("/home/user/file.txt").await.unwrap();
        assert_eq!(inode.name, "file.txt");

        // Also accessible via non-normalized paths
        let inode2 = fs.get_inode("/home//user/./file.txt").await.unwrap();
        assert_eq!(inode.id, inode2.id);
    }
}
