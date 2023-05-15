use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    /// Port to open worker on
    #[arg(short, long, default_value_t = 2727)]
    port: u16,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    parsimon::worker::start(args.port)?;
    Ok(())
}
