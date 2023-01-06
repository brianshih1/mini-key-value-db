use protobuf_codegen::Codegen;

// example: https://github.com/stepancheg/rust-protobuf/tree/master/protobuf-examples/customize-serde
fn main() {
    Codegen::new()
        .protoc()
        .includes(&["src/protos"])
        .input("src/protos/test.proto")
        .cargo_out_dir("gen")
        // .inputs(&["src/protos/test.proto"])
        // .customize_callback(GenSerde)
        .run_from_script();
}
