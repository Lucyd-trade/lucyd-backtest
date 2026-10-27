//! `new <name>`: a Cargo project with one strategy, ready to run against the engine.

use std::path::{Path, PathBuf};

use lucyd_backtest::{Error, Result};

const CARGO_TOML: &str = include_str!("../templates/new/Cargo.toml.tmpl");
const MAIN_RS: &str = include_str!("../templates/new/main.rs.tmpl");
const GITIGNORE: &str = include_str!("../templates/new/gitignore");

/// Package names Cargo accepts: ASCII letters, digits, `-` and `_`, not starting with a digit.
fn check_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        && !name.starts_with(|c: char| c.is_ascii_digit())
        && !["test", "lucyd-backtest", "lucyd_backtest"].contains(&name);
    if ok {
        Ok(())
    } else {
        Err(Error::InvalidRequest(format!(
            "project name {name:?}: letters, digits, - and _, not starting with a digit"
        )))
    }
}

/// Creates the project in `dir` (the name is its last part). With `engine_path`, the engine
/// dependency points at a local checkout instead of git.
pub fn create(dir: &Path, engine_path: Option<&Path>) -> Result<PathBuf> {
    let name = dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| Error::InvalidRequest(format!("{}: no project name", dir.display())))?;
    check_name(name)?;
    if dir.exists() && std::fs::read_dir(dir)?.next().is_some() {
        return Err(Error::InvalidRequest(format!(
            "{} exists and is not empty",
            dir.display()
        )));
    }
    let mut manifest = CARGO_TOML.replace("{{name}}", name);
    if let Some(p) = engine_path {
        let p = std::fs::canonicalize(p)?;
        let s = p.to_string_lossy().replace('\\', "/");
        let s = s.strip_prefix("//?/").unwrap_or(&s).to_string();
        let git = "lucyd-backtest = { git = \"https://github.com/lucyd-trade/lucyd-backtest\" }";
        debug_assert!(manifest.contains(git));
        manifest = manifest.replace(git, &format!("lucyd-backtest = {{ path = \"{s}\" }}"));
    }
    std::fs::create_dir_all(dir.join("src"))?;
    std::fs::write(dir.join("Cargo.toml"), manifest)?;
    std::fs::write(dir.join("src/main.rs"), MAIN_RS.replace("{{name}}", name))?;
    std::fs::write(dir.join(".gitignore"), GITIGNORE)?;
    Ok(dir.to_path_buf())
}
