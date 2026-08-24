fn main() {
    // Declare our real inputs. Without at least one `rerun-if-changed`, Cargo
    // falls back to re-running this script whenever *any* file in the package
    // changes (Cargo Book, "Build Scripts" → "Change Detection") — so a
    // one-line edit to router.rs re-ran both prost_build passes below before
    // rustc even started. Both protos are self-contained (no `import`), so
    // these two files are the complete input set. Mirrors crates/cri/build.rs.
    println!("cargo:rerun-if-changed=proto/openapiv2.proto");
    println!("cargo:rerun-if-changed=proto/openapiv3.proto");

    // Compile the gnostic OpenAPI v2 protobuf definition.
    // This generates Rust types matching the gnostic openapi_v2 proto used by
    // K8s client-go to parse /openapi/v2 responses.
    // K8s ref: vendor/github.com/google/gnostic-models/openapiv2/OpenAPIv2.proto
    prost_build::Config::new()
        .compile_protos(&["proto/openapiv2.proto"], &["proto/"])
        .expect("Failed to compile openapiv2.proto");

    // Compile the (minimal) gnostic OpenAPI v3 protobuf definition.
    // K8s client-go's `openapi3` client negotiates this with the
    // `application/com.github.proto-openapi.spec.v3@v1.0+protobuf` Accept
    // header. We model just the top-level Document / Info / Paths messages —
    // sufficient for the negotiation tests and forward-compatible with the
    // full gnostic schema (unknown proto3 fields are tolerated by readers).
    // K8s ref: staging/src/k8s.io/client-go/openapi3 and
    // vendor/github.com/google/gnostic-models/openapiv3/OpenAPIv3.proto.
    prost_build::Config::new()
        .compile_protos(&["proto/openapiv3.proto"], &["proto/"])
        .expect("Failed to compile openapiv3.proto");
}
