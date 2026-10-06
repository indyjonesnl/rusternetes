//! Generate the CSI v1 Node-service gRPC client (and a server, used by the
//! test fake driver) from `proto/csi/v1/csi.proto`. See that file for
//! provenance.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/csi/v1/csi.proto";

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto], &["proto"])?;

    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}
