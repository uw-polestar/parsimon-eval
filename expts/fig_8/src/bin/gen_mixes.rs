use std::{fs, path::PathBuf};

use clap::Parser;
use rand::prelude::*;
use sensitivity_analysis::mix::MixSpace;

#[derive(Debug, Parser)]
struct Opt {
    #[clap(long)]
    input: PathBuf,
    #[clap(long)]
    count: usize,
    #[clap(long, default_value_t = 0)]
    seed: u64,
    #[clap(long)]
    output: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();
    let mut rng = StdRng::seed_from_u64(opt.seed);
    let mix_space: MixSpace = serde_json::from_str(&fs::read_to_string(&opt.input)?)?;
    let mixes = mix_space.to_mixes(opt.count, &mut rng);
    fs::write(&opt.output, serde_json::to_string_pretty(&mixes)?)?;
    Ok(())
}
