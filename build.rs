use std::env;
use std::path::PathBuf;

fn main() {
    let header = "/usr/include/db5.3/db.h";

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={header}");
    println!("cargo:rustc-link-search=native=/usr/lib");
    println!("cargo:rustc-link-lib=db-5.3");

    let bindings = bindgen::Builder::default()
        .header(header)
        .clang_arg("-I/usr/include/db5.3")
        .derive_debug(false)
        .generate_comments(false)
        .layout_tests(false)
        .generate()
        .expect("failed to generate Berkeley DB bindings");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR should be set"));
    bindings
        .write_to_file(out_dir.join("db_bindings.rs"))
        .expect("failed to write Berkeley DB bindings");
}
