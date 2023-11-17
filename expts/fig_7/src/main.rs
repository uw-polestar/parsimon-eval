use clap::Parser;
use scaling::Experiment;

fn main() -> anyhow::Result<()> {
    let expt = Experiment::parse();
    expt.run()?;
    Ok(())
}
