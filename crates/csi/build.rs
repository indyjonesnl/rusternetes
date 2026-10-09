//! Generate the CSI v1 gRPC client and server (the server is used by test fake
//! drivers) from `proto/csi/v1/csi.proto`. See that file for provenance.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/csi/v1/csi.proto";
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto], &["proto"])?;
    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}
