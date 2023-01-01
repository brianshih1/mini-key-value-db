use protobuf_codegen::Codegen;

// // Use this in build.rs
// protobuf_codegen::Codegen::new()
//     // Use `protoc` parser, optional.
//     .protoc()
//     // Use `protoc-bin-vendored` bundled protoc command, optional.
//     .protoc_path(&protoc_bin_vendored::protoc_bin_path().unwrap())
//     // All inputs and imports from the inputs must reside in `includes` directories.
//     .includes(&["src/protos"])
//     // Inputs must reside in some of include paths.
//     .input("src/protos/apple.proto")
//     .input("src/protos/banana.proto")
//     // Specify output directory relative to Cargo output directory.
//     .cargo_out_dir("protos")
//     .run_from_script();

// example: https://github.com/stepancheg/rust-protobuf/tree/master/protobuf-examples/customize-serde
fn main() {
    Codegen::new()
        .protoc()
        .includes(&["src/protos"])
        .input("src/protos/wtf.proto")
        .cargo_out_dir("gen")
        // .inputs(&["src/protos/test.proto"])
        // .customize_callback(GenSerde)
        .run_from_script();
}
