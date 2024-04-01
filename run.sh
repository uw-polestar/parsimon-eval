cargo run --release -- help

# fig-7
cargo run --release -- --mix spec/0_config.mix_b_03_5.json ns3
cargo run --release -- --mix spec/0_config.mix_b_03_5.json pmn-m
cargo run --release -- --mix spec/0_config.mix_b_03_5.json mlsys

# fig-8
cargo run --release -- --root=./data --mixes spec/all_dctcp.mix.json ns3-config
cargo run --release -- --root=./data --mixes spec/all_dctcp.mix.json pmn-m
cargo run --release -- --root=./data --mixes spec/all_dctcp.mix.json mlsys

cargo flamegraph -- --mixes spec/0.mix.json mlsys

# counterfactual search
cargo run --release -- --root=./data --mixes spec/all_counterfactual.mix.json ns3-config
cargo run --release -- --root=./data --mixes spec/all_counterfactual.mix.json mlsys

# fig-8 gen json, remember to change the random seed in m3/parsimon-eval/expts/fig_8/src/bin/gen_mixes.rs
cargo run --bin gen_mixes -- --input spec/all_dctcp.mixspace.json --count 192 --output spec/all_dctcp.mix.json
cargo run --bin gen_mixes -- --input spec/all_counterfactual.mixspace.json --count 192 --output spec/all_counterfactual.mix.json