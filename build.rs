use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=migrations");
    println!("cargo:rerun-if-changed=lineage-ui/src");
    println!("cargo:rerun-if-changed=lineage-ui/package.json");
    println!("cargo:rerun-if-changed=lineage-ui/vite.config.ts");
    println!("cargo:rerun-if-changed=timeline-ui/src");
    println!("cargo:rerun-if-changed=timeline-ui/package.json");
    println!("cargo:rerun-if-changed=timeline-ui/vite.config.ts");

    build_ui_bundle("lineage-ui");
    build_ui_bundle("timeline-ui");
}

fn build_ui_bundle(package_dir: &str) {
    if !std::path::Path::new(package_dir)
        .join("package.json")
        .exists()
    {
        return;
    }
    if !std::path::Path::new(package_dir)
        .join("node_modules")
        .exists()
    {
        run_npm(package_dir, &["install"]);
    }
    run_npm(package_dir, &["run", "build"]);
}

fn run_npm(package_dir: &str, args: &[&str]) {
    let status = Command::new("npm")
        .arg("--prefix")
        .arg(package_dir)
        .args(args)
        .status()
        .unwrap_or_else(|err| panic!("failed to run npm for {package_dir}: {err}"));
    assert!(
        status.success(),
        "npm --prefix {package_dir} {} failed with status {status}",
        args.join(" ")
    );
}
