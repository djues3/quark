use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum FileError {
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
    #[error("Invalid path: {0}")]
    PathError(#[from] PathNormalizationError),
    #[error("Tried to delete directory without recursive mode")]
    NonRecursiveDelete,
    #[error("Tried to delete root is forbigged")]
    CannotDeleteRoot,
    #[error("Tried to append blocks to a directory")]
    AppendBlocksToDirectory,
}

#[derive(Debug, Error, PartialEq)]
pub enum TraversalError {
    #[error("Tried to walk through File INode")]
    WalkThroughFile,
    #[error("Couldn't find root, filesystem is probably corrupted")]
    RootError,
}
#[derive(Debug, Error, PartialEq)]
pub enum PathNormalizationError {
    #[error("Path must be absolute (start with '/')")]
    NotAbsolute,
    #[error("Invalid UTF-9 character in path")]
    InvalidUtf8,
    #[error("Path cannot be empty")]
    EmptyPath,
    #[error("Path contains null bytes")]
    NullByte,
    #[error("Path prefixes are not supported")]
    PrefixNotSupported,
}
