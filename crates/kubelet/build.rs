//! Generate the CSI v1 Node-service gRPC client (and a server, used by the
//! test fake driver) from `proto/csi/v1/csi.proto`. See that file for
//! provenance.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/csi/v1/csi.proto";

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto], &["proto"])?;

    // Plugin-registration (`pluginregistration.Registration`): client for the
    // kubelet plugin manager, server for the test fake plugin.
    let reg = "proto/pluginregistration/v1/api.proto";
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[reg], &["proto"])?;

    println!("cargo:rerun-if-changed={proto}");
    println!("cargo:rerun-if-changed={reg}");
    Ok(())
}
