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
    
    let mix_space: MixSpace = serde_json::from_str(&fs::read_to_string(&opt.input)?)?;
    let mut mixes_list = Vec::new();
    for param_seed in 3..4 {
        let mut rng = StdRng::seed_from_u64(opt.seed);
        let mut rng_2 = StdRng::seed_from_u64(param_seed);
        let mixes = mix_space.to_mixes(opt.count, &mut rng, &mut rng_2, param_seed as usize);
        mixes_list.extend(mixes);
    }
    // mixes_list.sort_by_key(|mix| mix.id);
    fs::write(&opt.output, serde_json::to_string_pretty(&mixes_list)?)?;
    Ok(())
}
