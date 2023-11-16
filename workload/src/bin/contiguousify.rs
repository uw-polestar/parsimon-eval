use std::{fs, path::PathBuf};

use clap::Parser;
use workload::fabric::Cluster;

#[derive(Debug, Parser)]
struct Opt {
    file: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();
    let mut cluster: Cluster = serde_json::from_str(&fs::read_to_string(&opt.file)?)?;
    cluster.contiguousify();
    fs::write(&opt.file, serde_json::to_string_pretty(&cluster)?)?;
    Ok(())
}
