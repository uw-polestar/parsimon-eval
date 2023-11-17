use clap::Parser;
use sensitivity_analysis::Experiment;

fn main() -> anyhow::Result<()> {
    let expt = Experiment::parse();
    expt.run()?;
    Ok(())
}
