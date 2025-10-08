use quark_master::fs::*;

use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .pretty()
        .with_thread_names(true)
        .with_max_level(tracing::Level::INFO)
        .init();

    let fs = FileSystem::default();
    fs.create_directory("/abcd/").await?;
    fs.create_directory("/abcd/a/").await?;
    fs.create_directory("/abcd/b/").await?;
    fs.create_file("/abcd.exe").await?;

    println!("\n\n\n\nTesting failure case");
    let res = fs.create_directory("/abcd.exe/b/ec/").await;
    println!("{:?}", res.unwrap_err());
    //println!("{res:?}");
    println!("\n\n\n\n\n\n");
    let res = fs.create_file("/abcd.exe/b.exe").await;
    println!("{res:?}");
    println!("\n\n\n\n\n\n");
    println!("Searching for inode");
    let inode = fs.get_inode("/abcd.exe").await?;
    println!("{inode:#?}");
    println!("\n\n\n\n\n\n");
    fs.create_file("/a").await?;
    fs.create_file("/bin").await?;
    fs.create_file("/boiot").await?;
    fs.create_file("/etc").await?;
    fs.create_file("/e").await?;
    let res = fs.list_directory("/").await?;
    println!("{res:#?}");

    Ok(())
}
