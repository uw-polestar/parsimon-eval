cargo run --release -- help
cargo clean

cargo run --release -- --mix spec/1.mix.json ns3
cargo run --release -- --mix spec/1.mix.json pmn-m
cargo run --release -- --mix spec/1.mix.json pmn-mc


cargo run --release -- --mixes spec/25.mix.json ns3
cargo run --release -- --mixes spec/25.mix.json pmn-m
cargo run --release -- --mixes spec/25.mix.json pmn-mc