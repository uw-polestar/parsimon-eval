cargo run --release -- help

# fig-7
cargo run --release -- --mix spec/1.mix.json ns3
cargo run --release -- --mix spec/1.mix.json pmn-m
cargo run --release -- --mix spec/1.mix.json pmn-mc
cargo run --release -- --mix spec/1.mix.json pmn-path
cargo run --release -- --mix spec/2_config.mix.json ns3

# fig-8
cargo run --release -- --root=./data --mixes spec/0.mix.json ns3-config
cargo run --release -- --root=./data --mixes spec/0.mix.json pmn-m
cargo run --release -- --root=./data --mixes spec/0.mix.json mlsys

cargo flamegraph -- --mixes spec/0.mix.json mlsys

# counterfactual search
cargo run --release -- --root=./data_test --mixes spec/0.mix.json ns3-config


# fig-8 gen json, remember to change the random seed in m3/parsimon-eval/expts/fig_8/src/bin/gen_mixes.rs
cargo run --bin gen_mixes -- --input spec/all_dctcp.mixspace.json --count 192 --output spec/all_dctcp.mix.json

cargo run --bin gen_mixes -- --input spec/all_counterfactual.mixspace.json --count 192 --output spec/all_counterfactual.mix.json

cargo run --bin gen_mixes -- --input spec/all_counterfactual.mixspace_dctcp.json --count 25 --output spec/all_counterfactual_dctcp.mix.json

cargo run --bin gen_mixes -- --input spec/all_counterfactual.mixspace_hpcc.json --count 25 --output spec/all_counterfactual_hpcc.mix.json

# test ns3 from parsimon-eval
cd /data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/src
cargo test --lib ns3

# test network from parsimon-core
cd /data1/lichenni/projects/flow_simulation/parsimon/crates/parsimon-core/src
cargo test --lib network

cargo update -p parsimon
