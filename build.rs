fn main() {
    println!("cargo:rustc-link-search=Path/to/static/library");
    println!("cargo:rustc-link-lib=static=dynapool");
}