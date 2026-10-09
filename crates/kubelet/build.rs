//! Generate the plugin-registration gRPC client/server. (The CSI v1 proto lives
//! in `crates/csi`.)

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Plugin-registration (`pluginregistration.Registration`): client for the
    // kubelet plugin manager, server for the test fake plugin.
    let reg = "proto/pluginregistration/v1/api.proto";
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[reg], &["proto"])?;

    println!("cargo:rerun-if-changed={reg}");
    Ok(())
}
