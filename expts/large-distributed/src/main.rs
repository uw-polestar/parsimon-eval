use clap::Parser;
use large_distributed::Experiment;

fn main() -> anyhow::Result<()> {
    let expt = Experiment::parse();
    expt.run()?;
    Ok(())
}
