fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("src/proto/hello.proto")?;
    tonic_prost_build::compile_protos("src/proto/slave.proto")?;
    Ok(())
}
