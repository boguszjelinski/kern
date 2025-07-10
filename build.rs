fn main() {
    println!("cargo:rustc-link-search=pool");
    println!("cargo:rustc-link-lib=static=dynapool");
}