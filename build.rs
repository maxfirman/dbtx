use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=migrations");
    println!("cargo:rerun-if-changed=lineage-ui/src");
    println!("cargo:rerun-if-changed=lineage-ui/package.json");
    println!("cargo:rerun-if-changed=lineage-ui/vite.config.ts");
    println!("cargo:rerun-if-changed=timeline-ui/src");
    println!("cargo:rerun-if-changed=timeline-ui/package.json");
    println!("cargo:rerun-if-changed=timeline-ui/vite.config.ts");

    // Build lineage UI if npm is available and source exists
    if std::path::Path::new("lineage-ui/package.json").exists() {
        if !std::path::Path::new("lineage-ui/node_modules").exists() {
            let _ = Command::new("npm")
                .args(["--prefix", "lineage-ui", "install"])
                .status();
        }
        let status = Command::new("npm")
            .args(["--prefix", "lineage-ui", "run", "build"])
            .status();
        if let Ok(s) = status
            && !s.success()
        {
            println!("cargo:warning=lineage-ui build failed");
        }
    }

    // Build timeline UI if npm is available and source exists
    if std::path::Path::new("timeline-ui/package.json").exists() {
        if !std::path::Path::new("timeline-ui/node_modules").exists() {
            let _ = Command::new("npm")
                .args(["--prefix", "timeline-ui", "install"])
                .status();
        }
        let status = Command::new("npm")
            .args(["--prefix", "timeline-ui", "run", "build"])
            .status();
        if let Ok(s) = status
            && !s.success()
        {
            println!("cargo:warning=timeline-ui build failed");
        }
    }
}
