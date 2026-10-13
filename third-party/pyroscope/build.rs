use std::env;
use std::process::Command;

fn main() {
    let rustc = env::var("RUSTC").unwrap();

    let output = Command::new(rustc)
        .arg("--version")
        .output()
        .expect("Failed to run rustc");

    let version_string =
        String::from_utf8(output.stdout).expect("rustc --version stdout is not utf8");

    println!(
        "cargo:rustc-env=PYROSCOPE__RUSTC_VERSION={}",
        version_string.trim()
    );
}
