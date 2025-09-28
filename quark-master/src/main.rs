use quark_master::fs::*;

use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .pretty()
        .with_thread_names(true)
        .with_max_level(tracing::Level::INFO)
        .init();

    let mut fs = FileSystem::default();
    let s = String::from("/abcd/");
    fs.create_directory(s).await?;
    let s = String::from("/abcd/a/");
    fs.create_directory(s).await?;
    let s = String::from("/abcd/b/");
    fs.create_directory(s).await?;
    fs.create_file(String::from("/abcd.exe")).await?;

    println!("\n\n\n\nTesting failure case");
    let s = String::from("/abcd.exe/b/ec/");
    let res = fs.create_directory(s).await;
    println!("{:?}", res.unwrap_err());
    //println!("{res:?}");
    println!("\n\n\n\n\n\n");
    let s = String::from("/abcd.exe/b.exe");
    let res = fs.create_file(s).await;
    println!("{res:?}");
    println!("\n\n\n\n\n\n");
    println!("Searching for inode");
    let inode = fs.get_inode("/abcd.exe".into()).await?;
    println!("{inode:#?}");
    Ok(())
}
