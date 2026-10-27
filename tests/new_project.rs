//! `new <name>`: the generated project. The build check is ignored by default (it compiles the
//! engine's dependencies): `cargo test --test new_project -- --ignored`.

use std::path::{Path, PathBuf};
use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_lucyd-backtest"))
}

fn engine() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn generate(dir: &Path, local: bool) {
    let mut c = bin();
    c.arg("new").arg(dir);
    if local {
        c.arg("--engine-path").arg(engine());
    }
    let out = c.output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn creates_the_files() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("my-strategy");
    generate(&dir, false);
    let manifest = std::fs::read_to_string(dir.join("Cargo.toml")).unwrap();
    assert!(manifest.contains("name = \"my-strategy\""));
    // the one line to switch to crates.io later
    assert!(
        manifest.contains(
            "lucyd-backtest = { git = \"https://github.com/lucyd-trade/lucyd-backtest\" }"
        )
    );
    let main = std::fs::read_to_string(dir.join("src/main.rs")).unwrap();
    assert!(main.contains("\"my-strategy\".into()"));
    assert!(main.contains("impl Strategy for MyStrategy"));
    assert!(!main.contains("{{"));
    assert!(dir.join(".gitignore").exists());

    // a local engine instead of git
    let local = tmp.path().join("local_one");
    generate(&local, true);
    let manifest = std::fs::read_to_string(local.join("Cargo.toml")).unwrap();
    assert!(manifest.contains("lucyd-backtest = { path = "));
    assert!(!manifest.contains("git = "));
}

#[test]
fn refuses_bad_names_and_full_folders() {
    let tmp = tempfile::tempdir().unwrap();
    for bad in ["1st", "with space", "lucyd-backtest"] {
        let out = bin().arg("new").arg(tmp.path().join(bad)).output().unwrap();
        assert!(!out.status.success(), "{bad} accepted");
    }
    let full = tmp.path().join("full");
    std::fs::create_dir_all(&full).unwrap();
    std::fs::write(full.join("x"), "x").unwrap();
    let out = bin().arg("new").arg(&full).output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("not empty"));
}

/// The generated project builds against this checkout. Uses the engine's lock file so no newer
/// dependency versions get pulled, and a target folder of its own under the engine's `target/`.
#[test]
#[ignore]
fn generated_project_builds() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("check_me");
    generate(&dir, true);
    std::fs::copy(engine().join("Cargo.lock"), dir.join("Cargo.lock")).unwrap();
    let out = Command::new(env!("CARGO"))
        .args(["check", "--quiet"])
        .current_dir(&dir)
        .env(
            "CARGO_TARGET_DIR",
            engine().join("target").join("new-project-check"),
        )
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
