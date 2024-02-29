cargo run --release -- help
cargo clean

# fig-7
cargo run --release -- --mix spec/1.mix.json ns3
cargo run --release -- --mix spec/1.mix.json pmn-m
cargo run --release -- --mix spec/1.mix.json pmn-mc
cargo run --release -- --mix spec/1.mix.json pmn-path
cargo run --release -- --mix spec/3.mix.json ns3

# fig-8
cargo run --release -- --mixes spec/0.mix.json ns3
cargo run --release -- --root=./data_test --mixes spec/4.mix.json mlsys
cargo run --release -- --mixes spec/25.mix.json pmn
cargo run --release -- --mixes spec/25.mix.json pmn-m
cargo run --release -- --mixes spec/25.mix.json pmn-mc
cargo run --release -- --mixes spec/25.mix.json pmn-path
cargo run --release -- --mixes spec/25.mix.json flowsim

cargo run --release -- --root=./data_test --mixes spec/0.mix.json ns3
cargo run --release -- --root=./data_test --mixes spec/63.mix.json flowsim

cargo run --release -- --mixes spec/all.mix.json ns3
cargo run --release -- --mixes spec/all.mix.json pmn-m
cargo run --release -- --mixes spec/all.mix.json pmn-mc
cargo run --release -- --mixes spec/all.mix.json mlsys

cargo run --release -- --mixes spec/test_mlsys.mix.json mlsys-param

cargo run --release -- --mixes spec/pmn_m.mix.json pmn-m-param

cargo run --release -- --root=./data_test --mixes spec/0.mix.json ns3
cargo run --release -- --root=./data_test --mixes spec/0.mix.json mlsys-test > test.log
cargo run --release -- --mixes spec/all_config.mix.json ns3-config

cargo run --release -- --mixes spec/mlsys_config.mix.json mlsys-config

cargo run --release -- --mixes spec/0.mix.json mlsys-config

cargo flamegraph -- --mixes spec/0.mix.json mlsys

# fig-8 gen specs
cargo run --bin gen_mixes -- --input spec/all_config.mixspace.json --count 192 --output spec/all_config.mix.json

PATH=$PATH:/data1/lichenni/software/anaconda3/envs/py27/bin

python gen_opt.py 1 192 1 1000 100 0 0 > gen_opt_1_1_1000_100_0_0.log

python gen_opt.py 1 192 1 1000 100 1 0 > gen_opt_1_1_1000_100_1_0.log

python gen_opt.py 1 192 1 1000 100 1 1 > gen_opt_1_1_1000_100_1_1.log

python gen_opt_loop.py 1 192 1 0 0 > gen_opt_1_1_0_0.log

python gen_opt_loop.py 1 192 1 1 0 > gen_opt_1_1_1_0.log

python gen_opt_loop.py 1 192 1 1 1 > gen_opt_1_1_1_1.log

python gen_sensitivity_path_param_k.py > gen_k_mlsys-param_e267_p500_l100_tx_nosamp.log

python gen_sensitivity_path_param.py > gen_mlsys-param_e267_p500_l100_tx_nosamp_1.log

python gen_sensitivity_path.py > gen_mlsys_e365_p500_l100_tx_nosamp_1.log

python gen_sensitivity_path.py > gen_mlsys-new_e28_p500_l100_tx_nosamp_1.log

# change the route calculation from ns3 to parsimon, which delivers the same result
# test ns3 from parsimon-eval
cd /data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/src
cargo test --lib ns3

# test network from parsimon-core
cd /data1/lichenni/projects/flow_simulation/parsimon/crates/parsimon-core/src
cargo test --lib network

cargo update -p parsimon
