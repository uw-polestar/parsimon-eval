cargo run --release -- help
cargo clean

cargo run --release -- --mix spec/1.mix.json ns3
cargo run --release -- --mix spec/1.mix.json pmn-m
cargo run --release -- --mix spec/1.mix.json pmn-mc
cargo run --release -- --mix spec/1.mix.json pmn-path


cargo run --release -- --mixes spec/25.mix.json ns3
cargo run --release -- --root=./data_test --mixes spec/4.mix.json mlsys
cargo run --release -- --mixes spec/25.mix.json pmn
cargo run --release -- --mixes spec/25.mix.json pmn-m
cargo run --release -- --mixes spec/25.mix.json pmn-mc
cargo run --release -- --mixes spec/25.mix.json pmn-path
cargo run --release -- --mixes spec/25.mix.json flowsim

cargo run --release -- --root=./data_test --mixes spec/63.mix.json ns3
cargo run --release -- --root=./data_test --mixes spec/63.mix.json flowsim

cargo run --release -- --mixes spec/all.mix.json ns3
cargo run --release -- --mixes spec/all.mix.json pmn-m
cargo run --release -- --mixes spec/all.mix.json pmn-mc
cargo run --release -- --mixes spec/all.mix.json mlsys

python run.py --root /data1/lichenni/projects/flow_simulation/parsimon-eval/expts/fig_8/data/25/ns3 --cc dctcp --trace flows --bw 10 --topo topology --fwin 18000 --base_rtt 14400

python2 run.py --root mix --cc dctcp --trace flow_parsimon --bw 10 --topo fat_parsimon

PATH=$PATH:/data1/lichenni/software/anaconda3/envs/py27/bin


CC='gcc-5' CXX='g++-5' ./waf configure --build-profile=optimized

CC='gcc-5' CXX='g++-5' ./waf configure --build-profile=debug --out=build/debug

export NS_LOG=PacketPathExample=info

./waf --run 'scratch/third mix_7/config_topology_flows_dctcp.txt'
./waf -d debug -out=debug.txt --run 'scratch/test'

https://github.com/kwzhao/High-Precision-Congestion-Control/compare/325635a6baf0131b8d46b3b4394c0a1d621f4aff...3b7ad3222598937dc0a5b423ba8f360489e24ce4

python test.py > gen_mlsys_s2_bt50_dedupe.log

python gen_opt.py 1 192 1 1000 100 0 0 > gen_opt_1_1_1000_100_0_0.log

python gen_opt.py 1 192 1 1000 100 1 0 > gen_opt_1_1_1000_100_1_0.log

python gen_opt.py 1 192 1 1000 100 1 1 > gen_opt_1_1_1000_100_1_1.log


python gen_opt_loop.py 1 192 1 0 0 > gen_opt_1_1_0_0.log

python gen_opt_loop.py 1 192 1 1 0 > gen_opt_1_1_1_0.log

python gen_opt_loop.py 1 192 1 1 1 > gen_opt_1_1_1_1.log
