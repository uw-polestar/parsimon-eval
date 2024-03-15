use std::{
    collections::HashSet,
    fmt::{self}, fs,
    io::{self, BufRead},
    path::{Path, PathBuf},
    time::Instant,
};

// use ns3_frontend::Ns3Simulation;
use parsimon::core::{
    network::{Flow, FlowId, Network, NodeId},
    opts::SimOpts,
    units::{Bytes, Mbps, Nanosecs},
};
use parsimon::impls::clustering::{
    self,
    feature::{self, DistsAndLoad},
    greedy::GreedyClustering,
};
use parsimon::impls::linksim::minim::MinimLink;
use rand::prelude::*;
use rayon::prelude::*;
use workload::{
    fabric::Cluster,
    flowgen::{FlowGenerator, StopWhen},
    spatial::SpatialData,
};

use crate::mix::{Mix, MixId, MixParam};

use rand::distributions::WeightedIndex;
use rustc_hash::{FxHashMap,FxHashSet};
use crate::mlsys::{
    Mlsys,
    ns3_clean,
};
use crate::ns3::Ns3Simulation;
use crate::ns3link::Ns3Link;

const NS3_DIR: &str = "../../../parsimon/backends/High-Precision-Congestion-Control/simulation";
const BASE_RTT: Nanosecs = Nanosecs::new(14_400);
const WINDOW: Bytes = Bytes::new(18_000);
const DCTCP_GAIN: f64 = 0.0625;
const DCTCP_AI: Mbps = Mbps::new(615);
const NR_PATHS_SAMPLED: usize = 500;
const NR_PATHS_SAMPLED_NS3: usize = 500;
// const NR_PARALLEL_PROCESSES: usize = 192;
// const INPUT_PERCENTILES: [f32; 20] = [0.01, 0.25, 0.40, 0.55, 0.70, 0.75, 0.80, 0.85, 0.90, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 1.0, 1.0];
// const INPUT_PERCENTILES: [f32; 30] = [0.00, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.85, 0.90, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98,0.982,0.984,0.986,0.988, 0.99,0.992,0.994,0.996,0.998, 1.0, 1.0];
// const INPUT_PERCENTILES: [f32; 29] = [0.00, 0.10, 0.20, 0.30, 0.40, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.92, 0.94, 0.96, 0.98, 0.982, 0.984, 0.986, 0.988, 0.99, 0.992, 0.994, 0.996, 0.998, 1.0, 1.0];
// const INPUT_PERCENTILES: [f32; 30] = [0.01, 0.10, 0.20, 0.30, 0.40, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.92, 0.94, 0.96, 0.98, 0.982, 0.984, 0.986, 0.988, 0.99, 0.992, 0.994, 0.996, 0.998, 0.999, 1.0, 1.0];
const NR_SIZE_BUCKETS: usize = 4;
const OUTPUT_LEN: usize = 100;
const FLOWS_ON_PATH_THRESHOLD: usize = 1;
const SAMPLE_MODE: usize = 1;
// const NR_FLOWS: usize = 100;
const NR_FLOWS: usize = 10_000_000;

const MLSYS_PATH: &str = "../../../fast-mmf-fattree";
const MODEL_SUFFIX: &str = "_e476";

#[derive(Debug, clap::Parser)]
pub struct Experiment {
    #[clap(long, default_value = "./data")]
    root: PathBuf,
    #[clap(long)]
    mixes: PathBuf,
    #[clap(long, default_value_t = 0)]
    seed: u64,
    #[clap(subcommand)]
    sim: SimKind
}

impl Experiment {
    pub fn run(&self) -> anyhow::Result<()> {
        let mixes: Vec<Mix> = serde_json::from_str(&fs::read_to_string(&self.mixes)?)?;
        // mixes=mixes.into_iter().rev().collect();
        
        // All ns3 simulations can run in parallel. Parsimon simulations are already massively
        // parallel, so they'll run one at a time to save memory.
        match self.sim {
            SimKind::Ns3 => {
                mixes.par_iter().try_for_each(|mix| self.run_ns3(mix))?;
                // let mix_list = mixes.chunks(NR_PARALLEL_PROCESSES).collect::<Vec<_>>();
                // for mix_tmp in &mix_list {
                //     mix_tmp.par_iter().try_for_each(|mix| self.run_ns3(mix))?;
                // }
            }
            SimKind::Ns3Config => {
                mixes.par_iter().try_for_each(|mix| self.run_ns3_config(mix))?;
                // let mix_list = mixes.chunks(NR_PARALLEL_PROCESSES).collect::<Vec<_>>();
                // println!("mix_list.len(): {}", mix_list.len());
                // for mix_tmp in &mix_list {
                //     mix_tmp.par_iter().try_for_each(|mix| self.run_ns3_config(mix))?;
                // }
            }
            SimKind::Ns3Param => {
                // let mixes_param: Vec<MixParam> = serde_json::from_str(&fs::read_to_string("spec/remain_param.mix.json")?)?;
                let mixes_param: Vec<MixParam> = serde_json::from_str(&fs::read_to_string("spec/test_param.mix.json")?)?;
                // mixes=mixes.into_iter().rev().collect();
                let mixed_combined:Vec<(Mix,MixParam)>=mixes.into_iter().zip(mixes_param.into_iter()).collect();
                
                // let mix_list = mixed_combined.chunks(NR_PARALLEL_PROCESSES).collect::<Vec<_>>();

                // for mix_tmp in &mix_list {
                //     mix_tmp.par_iter().try_for_each(|(mix,mix_param)| self.run_ns3_param(mix,mix_param))?;
                // }
                // mix_list[1].par_iter().try_for_each(|(mix,mix_param)| self.run_ns3_param(mix,mix_param))?;

                mixed_combined.par_iter().try_for_each(|(mix,mix_param)| self.run_ns3_param(mix,mix_param))?;
            }

            SimKind::Mlsys => {
                for mix in &mixes {
                    self.run_mlsys(mix)?;
                }
            }

            SimKind::MlsysParam => {
                let mixes_param: Vec<MixParam> = serde_json::from_str(&fs::read_to_string("spec/test_mlsys_param.mix.json")?)?;
                // mixes=mixes.into_iter().rev().collect();
                let mixed_combined:Vec<(Mix,MixParam)>=mixes.into_iter().zip(mixes_param.into_iter()).collect();

                for (mix,mix_param) in &mixed_combined {
                    self.run_mlsys_param(mix,mix_param)?;
                }
            }

            SimKind::MlsysTest => {
                for mix in &mixes {
                    self.run_mlsys_test(mix)?;
                }
            }
            
            SimKind::Ns3PathOne => {
                mixes.par_iter().try_for_each(|mix| self.run_ns3_path_one(mix))?;

                // let mix_list = mixes.chunks(NR_PARALLEL_PROCESSES).collect::<Vec<_>>();

                // for mix_tmp in &mix_list {
                //     mix_tmp
                //         .par_iter()
                //         .try_for_each(|mix| self.run_ns3_path_one(mix))?;
                // }
            }
            SimKind::Ns3PathAll => {
                // let mix_list = mixes.chunks(10).collect::<Vec<_>>();

                // for mix_tmp in &mix_list {
                //     mix_tmp
                //         .par_iter()
                //         .try_for_each(|mix| self.run_ns3_path_all(mix))?;
                // }
                for mix in &mixes {
                    self.run_ns3_path_all(mix)?;
                }
            }
            SimKind::Pmn => {
                println!("mixes.len(): {}", mixes.len());
                for mix in &mixes {
                    self.run_pmn(mix)?;
                }
            }
            SimKind::PmnM => {
                for mix in &mixes {
                    self.run_pmn_m(mix)?;
                }
            }
            SimKind::PmnMParam => {
                let mixes_param: Vec<MixParam> = serde_json::from_str(&fs::read_to_string("spec/pmn_m_param.mix.json")?)?;
                // mixes=mixes.into_iter().rev().collect();
                let mixed_combined:Vec<(Mix,MixParam)>=mixes.into_iter().zip(mixes_param.into_iter()).collect();

                for (mix,mix_param) in &mixed_combined {
                    self.run_pmn_m_param(mix,mix_param)?;
                }
            }
            SimKind::PmnMC => {
                for mix in &mixes {
                    self.run_pmn_mc(mix)?;
                }
            }
            SimKind::PmnMPath => {
                for mix in &mixes {
                    self.run_pmn_m_path(mix)?;
                }
            }
            
        }
        Ok(())
    }

    fn run_ns3(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::Ns3;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;

        let start = Instant::now(); // timer start
        let ns3 = Ns3Simulation::builder()
            .ns3_dir(NS3_DIR)
            .data_dir(self.sim_dir(mix, sim)?)
            .nodes(cluster.nodes().cloned().collect::<Vec<_>>())
            .links(cluster.links().cloned().collect::<Vec<_>>())
            // .window(WINDOW)
            .base_rtt(BASE_RTT)
            .flows(flows)
            .bfsz(mix.bfsz)
            .window(Bytes::new(mix.window))
            .enable_pfc(mix.enable_pfc)
            .cc_kind(mix.cc)
            .param_1(mix.param_1)
            .param_2(mix.param_2)
            .build();
        let records = ns3
            .run()?
            .into_iter()
            .map(|rec| Record {
                mix_id: mix.id,
                flow_id: rec.id,
                size: rec.size,
                slowdown: rec.slowdown(),
                sim,
            })
            .collect::<Vec<_>>();
        self.put_records(mix, sim, &records)?;

        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_elapsed(mix, sim, elapsed_secs)?;
        Ok(())
    }

    fn run_ns3_config(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::Ns3Config;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;

        let start = Instant::now(); // timer start
        let ns3 = Ns3Simulation::builder()
            .ns3_dir(NS3_DIR)
            // .data_dir(self.sim_dir(mix, sim)?)
            .data_dir(self.sim_dir_with_idx(mix, sim, mix.param_id)?)
            .nodes(cluster.nodes().cloned().collect::<Vec<_>>())
            .links(cluster.links().cloned().collect::<Vec<_>>())
            // .window(WINDOW)
            .base_rtt(BASE_RTT)
            .flows(flows)
            .bfsz(mix.bfsz)
            .window(Bytes::new(mix.window))
            .enable_pfc(mix.enable_pfc)
            .cc_kind(mix.cc)
            .param_1(mix.param_1)
            .param_2(mix.param_2)
            .build();
        let records = ns3
            .run()?
            .into_iter()
            .map(|rec| Record {
                mix_id: mix.id,
                flow_id: rec.id,
                size: rec.size,
                slowdown: rec.slowdown(),
                sim,
            })
            .collect::<Vec<_>>();
        self.put_records_with_idx(mix, sim, mix.param_id, &records)?;

        let elapsed_secs = start.elapsed().as_secs(); // timer end
        // self.put_elapsed(mix, sim, elapsed_secs)?;
        self.put_elapsed_with_idx(mix, sim, mix.param_id, elapsed_secs)?;
        Ok(())
    }

    fn run_ns3_param(&self, mix: &Mix, mix_param: &MixParam) -> anyhow::Result<()> {
        let sim = SimKind::Ns3Param;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;

        let start = Instant::now(); // timer start
        let ns3 = Ns3Simulation::builder()
            .ns3_dir(NS3_DIR)
            .data_dir(self.sim_dir(mix, sim)?)
            .nodes(cluster.nodes().cloned().collect::<Vec<_>>())
            .links(cluster.links().cloned().collect::<Vec<_>>())
            // .window(WINDOW)
            .base_rtt(BASE_RTT)
            .cc_kind(mix_param.cc)
            .window(Bytes::new(mix_param.window))
            .flows(flows)
            .build();

        let records = ns3
            .run()?
            .into_iter()
            .map(|rec| Record {
                mix_id: mix.id,
                flow_id: rec.id,
                size: rec.size,
                slowdown: rec.slowdown(),
                sim,
            })
            .collect::<Vec<_>>();
        self.put_records(mix, sim, &records)?;

        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_elapsed(mix, sim, elapsed_secs)?;
        Ok(())
    }

    fn run_ns3_path_one(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::Ns3PathOne;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;

        // read flows associated with a path
        let flow_path_map_file = self.flow_path_map_file(mix, sim)?;

        let start = Instant::now(); // timer start
        let (channel_to_flowid_map, flowid_to_path_map)= self.get_input_from_file(flow_path_map_file)?;

        let (path_to_flowid_map, _)= self.get_routes(flowid_to_path_map, &flows);

        let path = path_to_flowid_map
            .iter()
            .max_by_key(|x| x.1.len())
            .unwrap()
            .0;

        let flow_ids_in_f = path_to_flowid_map[path]
            .iter()
            .map(|x| FlowId::new(*x))
            .collect::<HashSet<_>>();

        let mut flow_ids_in_f_prime: HashSet<FlowId> = HashSet::new();
        for pair in path {
            if channel_to_flowid_map.contains_key(&pair) {
                flow_ids_in_f_prime.extend(channel_to_flowid_map[&pair].iter());
            }
        }

        let path_str = path
            .iter()
            .map(|&x| format!("{}-{}", x.0, x.1))
            .collect::<Vec<String>>()
            .join("|");
        let flow_ids_in_f_str = flow_ids_in_f
            .iter()
            .map(|&x| x.to_string())
            .collect::<Vec<String>>()
            .join(",");
        let flow_ids_in_f_prime_str = flow_ids_in_f_prime
            .iter()
            .map(|&x| x.to_string())
            .collect::<Vec<String>>()
            .join(",");
        self.put_path(
            mix,
            sim,
            format!(
                "{},{},{}\n{}\n{}",
                path_str,
                flow_ids_in_f.len(),
                flow_ids_in_f_prime.len(),
                flow_ids_in_f_str,
                flow_ids_in_f_prime_str
            ),
        )?;
        // println!("The selected path is ({:?}, {:?})", max_row,max_col);

        // get flows for a specific path
        let flows_remaining = flows
            .into_iter()
            .filter(|flow| flow_ids_in_f_prime.contains(&flow.id))
            .collect::<Vec<_>>();

        let ns3 = Ns3Simulation::builder()
            .ns3_dir(NS3_DIR)
            .data_dir(self.sim_dir(mix, sim)?)
            .nodes(cluster.nodes().cloned().collect::<Vec<_>>())
            .links(cluster.links().cloned().collect::<Vec<_>>())
            .window(WINDOW)
            .base_rtt(BASE_RTT)
            .flows(flows_remaining)
            .build();
        let records = ns3
            .run()?
            .into_iter()
            .filter(|rec| flow_ids_in_f.contains(&rec.id))
            .map(|rec| Record {
                mix_id: mix.id,
                flow_id: rec.id,
                size: rec.size,
                slowdown: rec.slowdown(),
                sim,
            })
            .collect::<Vec<_>>();
        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_elapsed(mix, sim, elapsed_secs)?;
        self.put_records(mix, sim, &records)?;
        Ok(())
    }

    fn run_ns3_path_all(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::Ns3PathAll;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;

        let start_1 = Instant::now(); // timer start
        let start_read = Instant::now(); // timer start
        // construct SimNetwork
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let network = Network::new(&nodes, &links)?;
        let network = network.into_simulations_path(flows.clone());
        let (channel_to_flowid_map, path_to_flowid_map): (
            &FxHashMap<(NodeId, NodeId), FxHashSet<FlowId>>,
            &FxHashMap<Vec<(NodeId, NodeId)>, FxHashSet<FlowId>>
        ) = match network.get_routes() {
            Some((channel_map, path_map)) => (channel_map, path_map),
            None => panic!("Routes not available"),
        };

        let mut path_to_flows_vec_sorted = path_to_flowid_map
            .iter()
            .filter(|(_, value)| value.len() >= FLOWS_ON_PATH_THRESHOLD)
            .collect::<Vec<_>>();
        path_to_flows_vec_sorted.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then(b.0[0].cmp(&a.0[0])));
        let elapsed_read= start_read.elapsed().as_secs();

        let start_sample= Instant::now(); // timer start
        let mut path_list: Vec<Vec<(NodeId, NodeId)>>;
        let mut path_counts: FxHashMap<Vec<(NodeId, NodeId)>, usize> = FxHashMap::default();

        let weights: Vec<usize> = path_to_flows_vec_sorted.iter()
        .map(|(_, flows)| flows.len()).collect();
        let weighted_index = WeightedIndex::new(weights).unwrap();

        let mut rng = StdRng::seed_from_u64(self.seed);
        (0..NR_PATHS_SAMPLED_NS3).for_each(|_| {
            let sampled_index = weighted_index.sample(&mut rng);
            let key = path_to_flows_vec_sorted[sampled_index].0.clone();
            // Update counts
            *path_counts.entry(key).or_insert(0) += 1;
        });

        // Derive the unique set of paths
        path_list = path_counts.clone().into_keys().collect();
        
        path_list.sort_by(|x, y| y.len().cmp(&x.len()).then(path_to_flowid_map[y].len().cmp(&path_to_flowid_map[x].len())));
        let elapsed_sample= start_sample.elapsed().as_secs();

        let start_path = Instant::now(); // timer start
        path_list
            .par_iter()
            .enumerate()
            .for_each(|(path_idx, path)| {
                let mut start_tmp = Instant::now();
                let flow_ids_in_f = path_to_flowid_map[path]
                    .iter()
                    .collect::<HashSet<_>>();
                let mut flow_ids_in_f_prime: HashSet<FlowId> = HashSet::new();
                for src_dst_pair in path.iter().skip(1) {
                    if let Some(flows_on_path) = channel_to_flowid_map.get(src_dst_pair) {
                        flow_ids_in_f_prime.extend(flows_on_path);
                    }
                }

                // get flows for a specific path
                let mut flows_remaining: Vec<Flow> = flow_ids_in_f_prime
                .iter()
                .filter_map(|&flow_id| flows.get(flow_id.as_usize()).cloned())
                .collect();
                flows_remaining.sort_by(|a, b| a.start.cmp(&b.start));

                let elapsed_secs_preprop = start_tmp.elapsed().as_secs();
                start_tmp= Instant::now();
                let data_dir=self.sim_dir_with_idx(mix, sim, path_idx).unwrap();
                let ns3 = Ns3Simulation::builder()
                    .ns3_dir(NS3_DIR)
                    .data_dir(data_dir.clone())
                    .nodes(cluster.nodes().cloned().collect::<Vec<_>>())
                    .links(cluster.links().cloned().collect::<Vec<_>>())
                    // .window(WINDOW)
                    .base_rtt(BASE_RTT)
                    .flows(flows_remaining)
                    .bfsz(mix.bfsz)
                    .window(Bytes::new(mix.window))
                    .enable_pfc(mix.enable_pfc)
                    .cc_kind(mix.cc)
                    .param_1(mix.param_1)
                    .param_2(mix.param_2)
                    .build();
                let records = ns3
                    .run()
                    .unwrap()
                    .into_iter()
                    .filter(|rec| flow_ids_in_f.contains(&rec.id))
                    .map(|rec| Record {
                        mix_id: mix.id,
                        flow_id: rec.id,
                        size: rec.size,
                        slowdown: rec.slowdown(),
                        sim,
                    })
                    .collect::<Vec<_>>();
                
                let elapsed_secs_mlsys = start_tmp.elapsed().as_secs();

                let path_str = path
                    .iter()
                    .map(|&x| format!("{}-{}", x.0, x.1))
                    .collect::<Vec<String>>()
                    .join("|");
                let flow_ids_in_f_str = flow_ids_in_f.iter().map(|&x| x.to_string()).collect::<Vec<String>>().join(",");
                self.put_path_with_idx(
                    mix,
                    sim,
                    path_idx,
                    format!(
                        "{},{},{},{}\n{},{}\n{}",
                        path_str,
                        flow_ids_in_f.len(),
                        flow_ids_in_f_prime.len(),
                        path_counts[path],
                        elapsed_secs_preprop,
                        elapsed_secs_mlsys,
                        flow_ids_in_f_str,
                        // flow_ids_in_f_prime_str
                    ),
                )
                .unwrap();
                self.put_records_with_idx(mix, sim, path_idx, &records)
                    .unwrap();
                ns3_clean(data_dir).unwrap();
            });
        let elapsed_path = start_path.elapsed().as_secs(); // timer end
        let elapsed_1 = start_1.elapsed().as_secs(); // timer end
        self.put_elapsed_str(mix, sim, format!("{},{},{},{}", elapsed_1, elapsed_read,elapsed_sample,elapsed_path))?;
        println!("{}: {}", mix.id,elapsed_1);
        Ok(())
    }

    fn run_pmn(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::Pmn;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;

        let start = Instant::now(); // timer start
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        
        let network = Network::new(&nodes, &links)?;
        let network = network.into_simulations(flows.clone());
        let loads = network.link_loads().collect::<Vec<_>>();
        let linksim = Ns3Link::builder()
            .root_dir(self.sim_dir(mix, sim)?)
            .ns3_dir(NS3_DIR)
            // .window(WINDOW)
            .base_rtt(BASE_RTT)
            .bfsz(mix.bfsz)
            .window(Bytes::new(mix.window))
            .enable_pfc(mix.enable_pfc)
            .cc_kind(mix.cc)
            .param_1(mix.param_1)
            .param_2(mix.param_2)
            .build();
        let sim_opts = SimOpts::builder().link_sim(linksim).build();
        let network = network.into_delays(sim_opts)?;
        let mut rng = StdRng::seed_from_u64(self.seed);
        let records: Vec<_> = flows
            .iter()
            .filter_map(|f| {
                network
                    .slowdown(f.size, (f.src, f.dst), &mut rng)
                    .map(|slowdown| Record {
                        mix_id: mix.id,
                        flow_id: f.id,
                        size: f.size,
                        slowdown,
                        sim,
                    })
            })
            .collect();
        self.put_records(mix, sim, &records)?;
        self.put_loads(mix, sim, &loads)?;
        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_elapsed(mix, sim, elapsed_secs)?;
        println!("{}: {}", mix.id, elapsed_secs);
        Ok(())
    }

    fn run_pmn_m(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::PmnM;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;

        let start = Instant::now(); // timer start
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let network = Network::new(&nodes, &links)?;
        let network = network.into_simulations(flows.clone());
        let loads = network.link_loads().collect::<Vec<_>>();
        let linksim = MinimLink::builder()
            .window(WINDOW)
            .dctcp_gain(DCTCP_GAIN)
            .dctcp_ai(DCTCP_AI)
            .build();
        let sim_opts = SimOpts::builder().link_sim(linksim).build();
        let network = network.into_delays(sim_opts)?;
        let mut rng = StdRng::seed_from_u64(self.seed);
        let records: Vec<_> = flows
            .iter()
            .filter_map(|f| {
                network
                    .slowdown(f.size, (f.src, f.dst), &mut rng)
                    .map(|slowdown| Record {
                        mix_id: mix.id,
                        flow_id: f.id,
                        size: f.size,
                        slowdown,
                        sim,
                    })
            })
            .collect();
        self.put_loads(mix, sim, &loads)?;
        self.put_records(mix, sim, &records)?;
        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_elapsed(mix, sim, elapsed_secs)?;
        Ok(())
    }

    fn run_pmn_m_param(&self, mix: &Mix, mix_param: &MixParam) -> anyhow::Result<()> {
        println!("{}: {}", mix.id,mix_param.window);
        let sim = SimKind::PmnMParam;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;

        let start = Instant::now(); // timer start
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let network = Network::new(&nodes, &links)?;
        let network = network.into_simulations(flows.clone());
        let loads = network.link_loads().collect::<Vec<_>>();
        let linksim = MinimLink::builder()
            .window(Bytes::new(mix_param.window))
            .dctcp_gain(DCTCP_GAIN)
            .dctcp_ai(DCTCP_AI)
            .build();
        let sim_opts = SimOpts::builder().link_sim(linksim).build();
        let network = network.into_delays(sim_opts)?;
        let mut rng = StdRng::seed_from_u64(self.seed);
        let records: Vec<_> = flows
            .iter()
            .filter_map(|f| {
                network
                    .slowdown(f.size, (f.src, f.dst), &mut rng)
                    .map(|slowdown| Record {
                        mix_id: mix.id,
                        flow_id: f.id,
                        size: f.size,
                        slowdown,
                        sim,
                    })
            })
            .collect();
        self.put_loads(mix, sim, &loads)?;
        self.put_records(mix, sim, &records)?;
        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_elapsed(mix, sim, elapsed_secs)?;
        Ok(())
    }

    fn run_pmn_m_path(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::PmnMPath;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;

        let start = Instant::now(); // timer start
        // construct SimNetwork
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let network = Network::new(&nodes, &links)?;
        let network = network.into_simulations(flows.clone());

        // get a specific path
        let path_file = self.path_one_file(mix, sim)?;
        let file = fs::File::open(path_file)?;

        let reader = io::BufReader::new(file);
        let mut path: Vec<(NodeId, NodeId)> = Vec::new();
        if let Some(Ok(first_line)) = reader.lines().next() {
            path = first_line
                .split(",")
                .collect::<Vec<_>>()
                .iter()
                .rev()
                .skip(2)
                .map(|x| {
                    x.split("-")
                        .map(|x| x.parse::<usize>().unwrap())
                        .collect::<Vec<_>>()
                })
                .map(|x| (NodeId::new(x[0]), NodeId::new(x[1])))
                .collect::<Vec<_>>();
        }
        // get flows for a specific path
        // let path = network.path(max_row, max_col, |choices| choices.first());
        path.sort();
        let edge_ids_in_path = path
            .iter()
            .skip(1)
            .map(|(src, dst)| network.find_edge(*src, *dst).unwrap())
            .collect::<HashSet<_>>();
        let flow_ids_in_channel = edge_ids_in_path
            .iter()
            .map(|edge_id| {
                network
                    .flows_on(*edge_id)
                    .unwrap()
                    .into_iter()
                    .map(|flow| flow.id)
                    .collect::<HashSet<_>>()
            })
            .collect::<Vec<HashSet<_>>>();
        let flow_ids_in_channel_2 = flow_ids_in_channel.clone();
        let flow_ids_in_f_prime = flow_ids_in_channel
            .into_iter()
            .flat_map(|x| x.into_iter())
            .collect::<HashSet<_>>();
        let flow_ids_in_f = flow_ids_in_channel_2
            .into_iter()
            .fold(None::<HashSet<_>>, |acc, set| {
                acc.map(|existing| existing.intersection(&set).cloned().collect())
                    .or(Some(set))
            })
            .unwrap_or_default();

        let flows_remaining = flows
            .into_iter()
            .filter(|flow| flow_ids_in_f_prime.contains(&flow.id))
            .collect::<Vec<_>>();

        let path_str = path
            .iter()
            .map(|&x| format!("{}-{}", x.0, x.1))
            .collect::<Vec<String>>()
            .join(",");
        let flow_ids_in_f_str = flow_ids_in_f
            .iter()
            .map(|&x| x.to_string())
            .collect::<Vec<String>>()
            .join(",");
        let flow_ids_in_f_prime_str = flow_ids_in_f_prime
            .iter()
            .map(|&x| x.to_string())
            .collect::<Vec<String>>()
            .join(",");
        self.put_path(
            mix,
            sim,
            format!(
                "{},{},{}\n{}\n{}",
                path_str,
                flow_ids_in_f.len(),
                flow_ids_in_f_prime.len(),
                flow_ids_in_f_str,
                flow_ids_in_f_prime_str
            ),
        )?;
        // println!("The selected path is ({:?}, {:?})", max_row,max_col);

        let network = Network::new(&nodes, &links)?;
        let network = network.into_simulations(flows_remaining.clone());
        let loads = network.link_loads().collect::<Vec<_>>();
        let linksim = MinimLink::builder()
            .window(WINDOW)
            .dctcp_gain(DCTCP_GAIN)
            .dctcp_ai(DCTCP_AI)
            .build();
        let sim_opts = SimOpts::builder().link_sim(linksim).build();
        let network = network.into_delays(sim_opts)?;
        let mut rng = StdRng::seed_from_u64(self.seed);
        let records: Vec<_> = flows_remaining
            .iter()
            .filter(|f| flow_ids_in_f.contains(&f.id))
            .filter_map(|f| {
                network
                    .slowdown(f.size, (f.src, f.dst), &mut rng)
                    .map(|slowdown| Record {
                        mix_id: mix.id,
                        flow_id: f.id,
                        size: f.size,
                        slowdown,
                        sim,
                    })
            })
            .collect();
        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_loads(mix, sim, &loads)?;
        self.put_elapsed(mix, sim, elapsed_secs)?;
        self.put_records(mix, sim, &records)?;
        Ok(())
    }

    fn run_mlsys(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::Mlsys;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;

        let start_1 = Instant::now(); // timer start
        let start_read = Instant::now(); // timer start
        // construct SimNetwork
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let network = Network::new(&nodes, &links)?;
        let network = network.into_simulations_path(flows.clone());
        let (channel_to_flowid_map, path_to_flowid_map): (
            &FxHashMap<(NodeId, NodeId), FxHashSet<FlowId>>,
            &FxHashMap<Vec<(NodeId, NodeId)>, FxHashSet<FlowId>>
        ) = match network.get_routes() {
            Some((channel_map, path_map)) => (channel_map, path_map),
            None => panic!("Routes not available"),
        };
        
        // // Collect lengths into a vector
        // let mut lengths: Vec<usize> = path_to_flowid_map.iter()
        // .map(|(_, value)| value.len())
        // .collect();

        // // Sort the vector to enable percentile calculation
        // lengths.sort_unstable();

        // // Calculate the 90th percentile index
        // // Note: subtract 1 because vector indices start at 0
        // let flows_on_path_threshold_idx = ((lengths.len() as f32) * 0.1).ceil() as usize - 1;
        // let flows_on_path_threshold= lengths[flows_on_path_threshold_idx];
        // println!("flows_on_path_threshold: {}", flows_on_path_threshold);
        
        let mut path_to_flows_vec_sorted = path_to_flowid_map
            .iter()
            .filter(|(_, value)| value.len() >= FLOWS_ON_PATH_THRESHOLD)
            .collect::<Vec<_>>();
        path_to_flows_vec_sorted.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then(b.0[0].cmp(&a.0[0])));
        let elapsed_read= start_read.elapsed().as_secs();

        let start_sample= Instant::now(); // timer start
        let mut path_list: Vec<Vec<(NodeId, NodeId)>>;
        let mut path_counts: FxHashMap<Vec<(NodeId, NodeId)>, usize> = FxHashMap::default();

        // path sampling: randomly select N flows, with the probability of a flow being selected proportional to the number of paths it is on
        let weights: Vec<usize> = path_to_flows_vec_sorted.iter()
        .map(|(_, flows)| flows.len()).collect();
        let weighted_index = WeightedIndex::new(weights).unwrap();

        let mut rng = StdRng::seed_from_u64(self.seed);
        (0..NR_PATHS_SAMPLED).for_each(|_| {
            let sampled_index = weighted_index.sample(&mut rng);
            let key = path_to_flows_vec_sorted[sampled_index].0.clone();
            // Update counts
            *path_counts.entry(key).or_insert(0) += 1;
        });

        // Derive the unique set of paths
        path_list = path_counts.clone().into_keys().collect();

        path_list.sort_by(|x, y| y.len().cmp(&x.len()).then(path_to_flowid_map[y].len().cmp(&path_to_flowid_map[x].len())));
        let elapsed_sample= start_sample.elapsed().as_secs();

        let start_path = Instant::now(); // timer start
        let results:Vec<_> = path_list
            .par_iter()
            .enumerate()
            .map(|(path_idx, path)| {
                let mut start_tmp = Instant::now();
                let mut flow_ids_in_f_prime: HashSet<FlowId> = HashSet::new();

                let mut flow_to_srcdst_map_in_flowsim: FxHashMap<FlowId, Vec<(usize, usize)>> = FxHashMap::default();
                let mut path_length = 1;
                for src_dst_pair in path.iter().skip(1) {
                    if let Some(flows_on_path) = channel_to_flowid_map.get(src_dst_pair) {
                        flow_ids_in_f_prime.extend(flows_on_path);
                        for &key_flowid in flows_on_path {
                            // println!("flow {} is on path {}", key_flowid, idx);
                            if let Some(count_vec) = flow_to_srcdst_map_in_flowsim.get_mut(&key_flowid) {
                                if count_vec.last().unwrap().1 != path_length - 1 {
                                    count_vec.push((path_length - 1, path_length));
                                }
                                else {
                                    count_vec.last_mut().unwrap().1 = path_length;
                                }
                            } else {
                                let tmp=vec![(path_length - 1, path_length)];
                                flow_to_srcdst_map_in_flowsim
                                    .insert(key_flowid, tmp);
                            }
                        }
                        path_length += 1;
                    }
                }
                // assert_eq!(path_length, path.len());

                // get flows for a specific path
                let mut flows_remaining: Vec<Flow> = flow_ids_in_f_prime
                .iter()
                .filter_map(|&flow_id| flows.get(flow_id.as_usize()).cloned())
                .collect();
                
                let mut flow_extra: Vec<Flow>=Vec::new();

                for flow in flows_remaining.iter_mut() {
                    if let Some(count_vec) = flow_to_srcdst_map_in_flowsim.get(&flow.id) {
                        flow.src = NodeId::new(count_vec[0].0);
                        flow.dst = NodeId::new(count_vec[0].1);
                        for i in 1..count_vec.len() {
                            let mut tmp=flow.clone();
                            tmp.src = NodeId::new(count_vec[i].0);
                            tmp.dst = NodeId::new(count_vec[i].1);
                            tmp.id=FlowId::new(flow_extra.len()+NR_FLOWS);
                            flow_extra.push(tmp);
                        }
                    }
                }
                flows_remaining.extend(flow_extra);

                flows_remaining.sort_by(|a, b| a.start.cmp(&b.start));

                let elapsed_secs_preprop = start_tmp.elapsed().as_secs();
                start_tmp= Instant::now();
                let mlsys = Mlsys::builder()
                    .script_path(MLSYS_PATH)
                    .data_dir(self.sim_dir_with_idx(mix, sim, path_idx).unwrap())
                    .flows(flows_remaining)
                    .seed(self.seed)
                    // .input_percentiles(INPUT_PERCENTILES.to_vec())
                    .input_percentiles((1..=100).map(|x| x as f32 / 100.0).collect())
                    .nr_size_buckets(NR_SIZE_BUCKETS)
                    .output_length(OUTPUT_LEN)
                    .bfsz(mix.bfsz)
                    .window(Bytes::new(mix.window))
                    .enable_pfc(mix.enable_pfc)
                    .cc_kind(mix.cc)
                    .param_1(mix.param_1)
                    .param_2(mix.param_2)
                    .model_suffix(MODEL_SUFFIX.to_string())
                    .build();
                let result = mlsys.run(path_length);

                let elapsed_secs_mlsys = start_tmp.elapsed().as_secs();
                
                let path_str = path
                    .iter()
                    .map(|&x| format!("{}-{}", x.0, x.1))
                    .collect::<Vec<String>>()
                    .join("|");
                let flow_ids_in_f_str = path_to_flowid_map[path].iter().map(|&x| x.to_string()).collect::<Vec<String>>().join(",");
                self.put_path_with_idx(
                    mix,
                    sim,
                    path_idx,
                    format!(
                        "{},{},{},{}\n{},{}\n{}",
                        path_str,
                        path_to_flowid_map[path].len(),
                        flow_ids_in_f_prime.len(),
                        path_counts[path],
                        elapsed_secs_preprop,
                        elapsed_secs_mlsys,
                        flow_ids_in_f_str
                    ),
                )
                .unwrap();
                result
            }).collect();
        println!("{}: {}", mix.id,results.len());

        let mut results_str = String::new();
        for result in results {
            let tmp=result.unwrap();
            for vec in tmp.iter() {
                results_str.push_str(&format!("{}\n", vec.iter().map(|&x| x.to_string()).collect::<Vec<_>>().join(",")));
            }
        }
        
        self.put_path(mix, sim, format!("{},{}\n{}", NR_PATHS_SAMPLED,path_list.len(),results_str))
                .unwrap();
        
        let elapsed_path = start_path.elapsed().as_secs(); // timer end
        let elapsed_1 = start_1.elapsed().as_secs(); // timer end

        self.put_elapsed_str(mix, sim, format!("{},{},{},{}", elapsed_1, elapsed_read,elapsed_sample,elapsed_path))?;
        Ok(())
    }

    fn run_mlsys_param(&self, mix: &Mix, mix_param: &MixParam) -> anyhow::Result<()> {
        let sim = SimKind::MlsysParam;
        let flows = self.flows(mix)?;
        // read flows associated with a path
        let flowid_to_flow_map: FxHashMap<FlowId, Flow> = flows
            .iter()
            .map(|flow| (flow.id, flow.clone()))
            .collect::<FxHashMap<_, _>>();
        let flow_path_map_file = self.flow_path_map_file(mix, sim)?;

        let start_1 = Instant::now(); // timer start
        // Create a buffered reader to efficiently read lines
        let (channel_to_flowid_map, flowid_to_path_map)= self.get_input_from_file(flow_path_map_file)?;
        let start_extra = Instant::now(); // timer start
        let (path_to_flowid_map, flowid_to_path_map_ordered)= self.get_routes(flowid_to_path_map, &flows);
        let elapsed_secs_extra = start_extra.elapsed().as_secs(); // timer end

        let path_to_flows_vec_sorted = path_to_flowid_map
            .iter()
            .filter(|(_, value)| value.len() >= FLOWS_ON_PATH_THRESHOLD)
            .collect::<Vec<_>>();
        // path_to_flows_vec_sorted.sort_by(|x, y| y.1.len().cmp(&x.1.len()).then(x.0.cmp(&y.0)));
        // let path_to_flows_vec_sorted: Vec<(&Vec<(NodeId, NodeId)>, &HashSet<usize>)> = {
        //     let mut temp_vec: Vec<_> = path_to_flowid_map
        //         .iter()
        //         .map(|(k, v)| (k, v))
        //         .collect();
        //     temp_vec.sort_by(|(_, a), (_, b)| b.len().cmp(&a.len()));
        //     temp_vec
        //         .iter()
        //         .take((temp_vec.len() as f64 * 0.8) as usize)
        //         .map(|&(k, v)| (k, v))
        //         .collect()
        // };

        let mut path_list: Vec<Vec<(NodeId, NodeId)>>;
        let mut flow_sampled_set: Vec<&usize>=Vec::new();
        let mut path_counts: FxHashMap<Vec<(NodeId, NodeId)>, usize> = FxHashMap::default();

        // Sample-1: randomly select N flows, with the probability of a flow being selected proportional to the number of paths it is on
        if SAMPLE_MODE==1{
            let weights: Vec<usize> = path_to_flows_vec_sorted.iter()
            .map(|(_, flows)| flows.len()).collect();
            let weighted_index = WeightedIndex::new(weights).unwrap();

            let mut rng = StdRng::seed_from_u64(self.seed);
            (0..NR_PATHS_SAMPLED).for_each(|_| {
                let sampled_index = weighted_index.sample(&mut rng);
                let key = path_to_flows_vec_sorted[sampled_index].0.clone();
                // Update counts
                *path_counts.entry(key).or_insert(0) += 1;
            });

            // Derive the unique set of paths
            path_list = path_counts.clone().into_keys().collect();
        }
        else if SAMPLE_MODE==2{
            // Sample-2: randomly select N flows, and then collect the paths they are on
            let flowid_pool= path_to_flows_vec_sorted
                .iter()
                .flat_map(|&(_, flows)| flows.iter());

            let mut rng = StdRng::seed_from_u64(self.seed);
            flow_sampled_set=flowid_pool.choose_multiple(&mut rng, NR_PATHS_SAMPLED);

            let path_list_ori = flow_sampled_set.iter()
            .map(|&flow_id| flowid_to_path_map_ordered[&flow_id].clone());
            
            path_list=path_list_ori.clone().collect::<HashSet<_>>().into_iter().collect::<Vec<_>>();

            path_counts = path_list_ori
            .fold(FxHashMap::default(), |mut acc, path| {
                *acc.entry(path.clone()).or_insert(0) += 1;
                acc
            });
        }
        else{
            panic!("invalid sample mode");
        }

        // path_list.sort_by(|x, y| path_to_flowid_map[y].len().cmp(&path_to_flowid_map[x].len()));
        path_list.sort_by(|x, y| y.len().cmp(&x.len()).then(path_to_flowid_map[y].len().cmp(&path_to_flowid_map[x].len())));

        let start_2 = Instant::now(); // timer start
        let results:Vec<_> = path_list
            .par_iter()
            .enumerate()
            .map(|(path_idx, path)| {
                let mut start_tmp = Instant::now();
                let mut flow_ids_in_f_prime: HashSet<FlowId> = HashSet::new();

                let mut flow_to_srcdst_map_in_flowsim: FxHashMap<FlowId, Vec<(usize, usize)>> = FxHashMap::default();
                let mut path_length = 1;
                for src_dst_pair in path.iter().skip(1) {
                    if let Some(flows_on_path) = channel_to_flowid_map.get(src_dst_pair) {
                        flow_ids_in_f_prime.extend(flows_on_path);
                        for &key_flowid in flows_on_path {
                            // println!("flow {} is on path {}", key_flowid, idx);
                            if let Some(count_vec) = flow_to_srcdst_map_in_flowsim.get_mut(&key_flowid) {
                                if count_vec.last().unwrap().1 != path_length - 1 {
                                    count_vec.push((path_length - 1, path_length));
                                }
                                else {
                                    count_vec.last_mut().unwrap().1 = path_length;
                                }
                            } else {
                                let tmp=vec![(path_length - 1, path_length)];
                                flow_to_srcdst_map_in_flowsim
                                    .insert(key_flowid, tmp);
                            }
                        }
                        path_length += 1;
                    }
                }
                // assert_eq!(path_length, path.len());

                // get flows for a specific path
                let mut flows_remaining: Vec<Flow> = flow_ids_in_f_prime
                .iter()
                .filter_map(|&flow_id| flowid_to_flow_map.get(&flow_id).cloned())
                .collect();
                
                let mut flow_extra: Vec<Flow>=Vec::new();

                for flow in flows_remaining.iter_mut() {
                    if let Some(count_vec) = flow_to_srcdst_map_in_flowsim.get(&flow.id) {
                        flow.src = NodeId::new(count_vec[0].0);
                        flow.dst = NodeId::new(count_vec[0].1);
                        for i in 1..count_vec.len() {
                            let mut tmp=flow.clone();
                            tmp.src = NodeId::new(count_vec[i].0);
                            tmp.dst = NodeId::new(count_vec[i].1);
                            tmp.id=FlowId::new(flow_extra.len()+NR_FLOWS);
                            flow_extra.push(tmp);
                        }
                    }
                }
                flows_remaining.extend(flow_extra);

                flows_remaining.sort_by(|a, b| a.start.cmp(&b.start));

                let elapsed_secs_preprop = start_tmp.elapsed().as_secs();
                start_tmp= Instant::now();
                let mlsys = Mlsys::builder()
                    .script_path(MLSYS_PATH)
                    .data_dir(self.sim_dir_with_idx(mix, sim, path_idx).unwrap())
                    .flows(flows_remaining)
                    .seed(self.seed)
                    // .input_percentiles(INPUT_PERCENTILES.to_vec())
                    .input_percentiles((1..=100).map(|x| x as f32 / 100.0).collect())
                    .nr_size_buckets(NR_SIZE_BUCKETS)
                    .output_length(OUTPUT_LEN)
                    .cc_kind(mix_param.cc)
                    .window(Bytes::new(mix_param.window))
                    .build();
                let result = mlsys.run(path_length);

                let elapsed_secs_mlsys = start_tmp.elapsed().as_secs();
                
                let path_str = path
                    .iter()
                    .map(|&x| format!("{}-{}", x.0, x.1))
                    .collect::<Vec<String>>()
                    .join("|");
                let flow_ids_in_f_str = path_to_flowid_map[path].iter().map(|&x| x.to_string()).collect::<Vec<String>>().join(",");
                self.put_path_with_idx(
                    mix,
                    sim,
                    path_idx,
                    format!(
                        "{},{},{},{}\n{},{}\n{}",
                        path_str,
                        path_to_flowid_map[path].len(),
                        flow_ids_in_f_prime.len(),
                        path_counts[path],
                        elapsed_secs_preprop,
                        elapsed_secs_mlsys,
                        flow_ids_in_f_str
                    ),
                )
                .unwrap();
                result
            }).collect();
        println!("{}: {}", mix.id,results.len());

        let mut results_str = String::new();
        for result in results {
            let tmp=result.unwrap();
            for vec in tmp.iter() {
                results_str.push_str(&format!("{}\n", vec.iter().map(|&x| x.to_string()).collect::<Vec<_>>().join(",")));
            }
        }
        
        if SAMPLE_MODE==1{
            self.put_path(mix, sim, format!("{},{}\n{}", NR_PATHS_SAMPLED,path_list.len(),results_str))
                .unwrap();
        }
        else if SAMPLE_MODE==2{
            let flow_sampled_str=flow_sampled_set.iter().map(|&x| x.to_string()).collect::<Vec<_>>().join(",");
            self.put_path(mix, sim, format!("{},{}\n{}{}", NR_PATHS_SAMPLED,path_list.len(),results_str,flow_sampled_str))
            .unwrap();
        }
        else{
            panic!("invalid sample mode");
        }

        let elapsed_secs_2 = start_2.elapsed().as_secs(); // timer end
        let elapsed_secs_1 = start_1.elapsed().as_secs(); // timer end
        
        self.put_elapsed_str(mix, sim, format!("{},{},{}", elapsed_secs_1, elapsed_secs_2,elapsed_secs_extra))?;
        Ok(())
    }

    fn run_mlsys_test(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::MlsysTest;
        let flows = self.flows(mix)?;

        let start_2 = Instant::now(); // timer start
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        // construct SimNetwork
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let network = Network::new(&nodes, &links)?;
        let network = network.into_simulations_path(flows.clone());
        // let path_to_flowid_map_pmn:FxHashMap<Vec<(NodeId, NodeId)>, FxHashSet<FlowId>> = network.get_path_to_flowid_map().unwrap().clone();
        let (channel_to_flowid_map_pmn, path_to_flowid_map_pmn): (
            &FxHashMap<(NodeId, NodeId), FxHashSet<FlowId>>,
            &FxHashMap<Vec<(NodeId, NodeId)>, FxHashSet<FlowId>>
        ) = match network.get_routes() {
            Some((channel_map, path_map)) => (channel_map, path_map),
            None => panic!("Routes not available"),
        };
        let mut channel_to_flowid_map_pmn_sorted = channel_to_flowid_map_pmn
            .iter()
            .collect::<Vec<_>>();
        channel_to_flowid_map_pmn_sorted.sort_by(|x, y| x.0.cmp(&y.0));
        let mut path_to_flows_vec_pmn_sorted = path_to_flowid_map_pmn
            .iter()
            .filter(|(_, value)| value.len() >= FLOWS_ON_PATH_THRESHOLD)
            .collect::<Vec<_>>();
        path_to_flows_vec_pmn_sorted.sort_by(|x, y| y.1.len().cmp(&x.1.len()).then(x.0.cmp(&y.0)));
        println!("Path to FlowID Map length: {}", path_to_flows_vec_pmn_sorted.len());
       
        let elapsed_secs_2 = start_2.elapsed().as_secs(); // timer end
        let mut results_str_pmn_c = String::new();
        
        for (index, (nodes, hash_set)) in channel_to_flowid_map_pmn_sorted.iter().enumerate() {
            results_str_pmn_c.push_str(&format!("Link-{}, ", index));
            
            // Append nodes
            results_str_pmn_c.push_str("[");
            results_str_pmn_c.push_str(&format!("({}, {}) ", nodes.0, nodes.1));
            
            // Append hash set
            results_str_pmn_c.push_str("{");
            let mut results_vec: Vec<_> = hash_set.iter().collect();
            results_vec.sort(); // Sort the vector
            for value in results_vec.iter() {
                results_str_pmn_c.push_str(&format!("{}, ", value));
            }
            results_str_pmn_c.push_str("}\n");
        }

        let mut results_str_pmn = String::new();
        
        for (index, (nodes, hash_set)) in path_to_flows_vec_pmn_sorted.iter().enumerate() {
            results_str_pmn.push_str(&format!("Path-{}, ", index));
            
            // Append nodes
            results_str_pmn.push_str("[");
            for (node_a, node_b) in nodes.iter() {
                results_str_pmn.push_str(&format!("({}, {}) ", node_a, node_b));
            }
            results_str_pmn.push_str("], ");
            
            // Append hash set
            results_str_pmn.push_str("{");
            let mut results_vec: Vec<_> = hash_set.iter().collect();
            results_vec.sort(); // Sort the vector
            for value in results_vec.iter() {
                results_str_pmn.push_str(&format!("{}, ", value));
            }
            results_str_pmn.push_str("}\n");
        }
        self.put_path_with_idx(
            mix,
            sim,
            0,
            format!(
                "{},{}\n{}\n{}", NR_PATHS_SAMPLED,path_to_flowid_map_pmn.len(),results_str_pmn_c,results_str_pmn
            ),
        )
        .unwrap();

        // read flows associated with a path
        let start_1 = Instant::now(); // timer start
        let flow_path_map_file = self.flow_path_map_file(mix, sim)?;
        let (channel_to_flowid_map, flowid_to_path_map)= self.get_input_from_file(flow_path_map_file)?;

        let start_extra = Instant::now(); // timer start
        let (path_to_flowid_map, _)= self.get_routes(flowid_to_path_map, &flows);
        let elapsed_secs_extra = start_extra.elapsed().as_secs(); // timer end

        let mut channel_to_flowid_map_sorted = channel_to_flowid_map
            .iter()
            .collect::<Vec<_>>();
        channel_to_flowid_map_sorted.sort_by(|x, y| x.0.cmp(&y.0));

        let mut path_to_flows_vec_sorted = path_to_flowid_map
            .iter()
            .filter(|(_, value)| value.len() >= FLOWS_ON_PATH_THRESHOLD)
            .collect::<Vec<_>>();
        path_to_flows_vec_sorted.sort_by(|x, y| y.1.len().cmp(&x.1.len()).then(x.0.cmp(&y.0)));
        let elapsed_secs_1 = start_1.elapsed().as_secs(); // timer end
            
        self.put_elapsed_str(mix, sim, format!("{},{},{}", elapsed_secs_1,elapsed_secs_2,elapsed_secs_extra))?;

        let mut results_str_c = String::new();
        
        for (index, (nodes, hash_set)) in channel_to_flowid_map_sorted.iter().enumerate() {
            results_str_c.push_str(&format!("Link-{}, ", index));
            
            // Append nodes
            results_str_c.push_str("[");
            results_str_c.push_str(&format!("({}, {}) ", nodes.0, nodes.1));
            
            // Append hash set
            results_str_c.push_str("{");
            let mut results_vec: Vec<_> = hash_set.iter().collect();
            results_vec.sort(); // Sort the vector
            for value in results_vec.iter() {
                results_str_c.push_str(&format!("{}, ", value));
            }
            results_str_c.push_str("}\n");
        }

        let mut results_str = String::new();
    
        for (index, (nodes, hash_set)) in path_to_flows_vec_sorted.iter().enumerate() {
            results_str.push_str(&format!("Path-{}, ", index));
            
            // Append nodes
            results_str.push_str("[");
            for (node_a, node_b) in nodes.iter() {
                results_str.push_str(&format!("({}, {}) ", node_a, node_b));
            }
            results_str.push_str("], ");
            
            // Append hash set
            results_str.push_str("{");
            let mut results_vec: Vec<_> = hash_set.iter().collect();
            results_vec.sort(); // Sort the vector
            for value in results_vec.iter() {
                results_str.push_str(&format!("{}, ", value));
            }
            results_str.push_str("}\n");
        }
        self.put_path_with_idx(
            mix,
            sim,
            1,
            format!(
                "{},{}\n{}\n{}", NR_PATHS_SAMPLED,path_to_flows_vec_sorted.len(),results_str_c,results_str
            ),
        )
        .unwrap();
        // self.put_path(mix, sim, format!("{},{}\n{}\n{}", NR_PATHS_SAMPLED,path_to_flowid_map.len(),results_str_pmn,results_str))
        //         .unwrap();
            
        Ok(())
    }
    
    fn run_pmn_mc(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::PmnMC;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let start = Instant::now(); // timer start
        let network = Network::new(&nodes, &links)?;
        let mut network = network.into_simulations(flows.clone());
        let loads = network.link_loads().collect::<Vec<_>>();
        let clusterer = GreedyClustering::new(feature::dists_and_load, is_close_enough);
        network.cluster(&clusterer);
        let nr_clusters = network.clusters().len();
        let frac = nr_clusters as f64 / (links.len() * 2) as f64;
        let linksim = MinimLink::builder()
            .window(WINDOW)
            .dctcp_gain(DCTCP_GAIN)
            .dctcp_ai(DCTCP_AI)
            .build();
        let sim_opts = SimOpts::builder().link_sim(linksim).build();
        let network = network.into_delays(sim_opts)?;
        let mut rng = StdRng::seed_from_u64(self.seed);
        let records: Vec<_> = flows
            .iter()
            .filter_map(|f| {
                network
                    .slowdown(f.size, (f.src, f.dst), &mut rng)
                    .map(|slowdown| Record {
                        mix_id: mix.id,
                        flow_id: f.id,
                        size: f.size,
                        slowdown,
                        sim,
                    })
            })
            .collect();
        self.put_loads(mix, sim, &loads)?;
        self.put_clustering(mix, sim, frac)?;
        self.put_records(mix, sim, &records)?;
        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_elapsed(mix, sim, elapsed_secs)?;
        Ok(())
    }

    fn flows(&self, mix: &Mix) -> anyhow::Result<Vec<Flow>> {
        let path = self.flow_file(mix)?;
        if !path.exists() {
            self.gen_flows(mix, &path)?;
        }
        let flows = parsimon::utils::read_flows(&path)?;
        Ok(flows)
    }

    fn gen_flows(&self, mix: &Mix, to: impl AsRef<Path>) -> anyhow::Result<()> {
        let spatial: SpatialData = serde_json::from_str(&fs::read_to_string(&mix.spatial)?)?;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let size_dist = utils::read_ecdf(&mix.size_dist)?;
        let flowgen = FlowGenerator::builder()
            .spatial_data(spatial)
            .cluster(cluster)
            .size_dist(size_dist)
            .lognorm_sigma(mix.lognorm_sigma)
            .max_load(mix.max_load)
            .stop_when(StopWhen::NrFlows(NR_FLOWS))
            .seed(self.seed)
            .build();
        let flows = flowgen.generate();
        let s = serde_json::to_string(&flows)?;
        fs::write(&to, s)?;
        Ok(())
    }

    fn put_records(&self, mix: &Mix, sim: SimKind, records: &[Record]) -> anyhow::Result<()> {
        let path = self.record_file(mix, sim)?;
        let mut wtr = csv::Writer::from_path(path)?;
        for record in records {
            wtr.serialize(record)?;
        }
        wtr.flush()?;
        Ok(())
    }

    fn put_records_with_idx(
        &self,
        mix: &Mix,
        sim: SimKind,
        path_idx: usize,
        records: &[Record],
    ) -> anyhow::Result<()> {
        let path = self.record_file_with_idx(mix, sim, path_idx)?;
        let mut wtr = csv::Writer::from_path(path)?;
        for record in records {
            wtr.serialize(record)?;
        }
        wtr.flush()?;
        Ok(())
    }

    fn put_elapsed(&self, mix: &Mix, sim: SimKind, secs: u64) -> anyhow::Result<()> {
        fs::write(self.elapsed_file(mix, sim)?, secs.to_string())?;
        Ok(())
    }

    fn put_elapsed_with_idx(&self, mix: &Mix, sim: SimKind, path_idx: usize, secs: u64) -> anyhow::Result<()> {
        fs::write(self.elapsed_file_with_idx(mix, sim, path_idx)?, secs.to_string())?;
        Ok(())
    }

    fn put_elapsed_str(&self, mix: &Mix, sim: SimKind, secs: String) -> anyhow::Result<()> {
        fs::write(self.elapsed_file(mix, sim)?, secs)?;
        Ok(())
    }

    fn put_path(&self, mix: &Mix, sim: SimKind, path_str: String) -> anyhow::Result<()> {
        fs::write(self.path_file(mix, sim)?, path_str)?;
        Ok(())
    }
    
    fn put_path_with_idx(
        &self,
        mix: &Mix,
        sim: SimKind,
        path_idx: usize,
        path_str: String,
    ) -> anyhow::Result<()> {
        fs::write(self.path_file_with_idx(mix, sim, path_idx)?, path_str)?;
        Ok(())
    }

    fn put_clustering(&self, mix: &Mix, sim: SimKind, frac: f64) -> anyhow::Result<()> {
        fs::write(self.clustering_file(mix, sim)?, frac.to_string())?;
        Ok(())
    }

    fn put_loads(&self, mix: &Mix, sim: SimKind, loads: &[f64]) -> anyhow::Result<()> {
        let s = serde_json::to_string(&loads)?;
        fs::write(self.load_file(mix, sim)?, s)?;
        Ok(())
    }

    fn mix_dir(&self, mix: &Mix) -> anyhow::Result<PathBuf> {
        let dir = [self.root.as_path(), mix.id.to_string().as_ref()]
            .into_iter()
            .collect();
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    fn sim_dir(&self, mix: &Mix, sim: SimKind) -> anyhow::Result<PathBuf> {
        let dir = [self.mix_dir(mix)?.as_path(), sim.to_string().as_ref()]
            .into_iter()
            .collect();
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }
    
    fn sim_dir_with_idx(
        &self,
        mix: &Mix,
        sim: SimKind,
        path_idx: usize,
    ) -> anyhow::Result<PathBuf> {
        let dir = [
            self.mix_dir(mix)?.as_path(),
            sim.to_string().as_ref(),
            path_idx.to_string().as_ref(),
        ]
        .into_iter()
        .collect();
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    fn flow_file(&self, mix: &Mix) -> anyhow::Result<PathBuf> {
        let file = [self.mix_dir(mix)?.as_path(), "flows.json".as_ref()]
            .into_iter()
            .collect();
        Ok(file)
    }

    fn path_one_file(&self, mix: &Mix, sim: SimKind) -> anyhow::Result<PathBuf> {
        let file = [
            self.sim_dir(mix, sim)?.as_path(),
            "../ns3-path-one/path.txt".as_ref(),
        ]
        .into_iter()
        .collect();
        Ok(file)
    }

    // fn path_all_with_idx_file(
    //     &self,
    //     mix: &Mix,
    //     sim: SimKind,
    //     path_idx: usize,
    // ) -> anyhow::Result<PathBuf> {
    //     let file = [
    //         self.sim_dir(mix, sim)?.as_path(),
    //         format!("../ns3-path-all/path_{}.txt", path_idx).as_ref(),
    //     ]
    //     .into_iter()
    //     .collect();
    //     Ok(file)
    // }

    // fn flow_on_path_file(&self, mix: &Mix, sim: SimKind) -> anyhow::Result<PathBuf> {
    //     let file = [
    //         self.sim_dir(mix, sim)?.as_path(),
    //         "../ns3/flows_on_path.txt".as_ref(),
    //     ]
    //     .into_iter()
    //     .collect();
    //     Ok(file)
    // }

    fn flow_path_map_file(&self, mix: &Mix, sim: SimKind) -> anyhow::Result<PathBuf> {
        let file = [
            self.sim_dir(mix, sim)?.as_path(),
            "../ns3/flows_path_map.txt".as_ref(),
        ]
        .into_iter()
        .collect();
        Ok(file)
    }

    fn path_file(&self, mix: &Mix, sim: SimKind) -> anyhow::Result<PathBuf> {
        let file = [self.sim_dir(mix, sim)?.as_path(), "path.txt".as_ref()]
            .into_iter()
            .collect();
        Ok(file)
    }

    fn path_file_with_idx(
        &self,
        mix: &Mix,
        sim: SimKind,
        path_idx: usize,
    ) -> anyhow::Result<PathBuf> {
        let file = [
            self.sim_dir(mix, sim)?.as_path(),
            format!("path_{}.txt", path_idx).as_ref(),
        ]
        .into_iter()
        .collect();
        Ok(file)
    }

    fn record_file(&self, mix: &Mix, sim: SimKind) -> anyhow::Result<PathBuf> {
        let file = [self.sim_dir(mix, sim)?.as_path(), "records.csv".as_ref()]
            .into_iter()
            .collect();
        Ok(file)
    }
    fn record_file_with_idx(
        &self,
        mix: &Mix,
        sim: SimKind,
        path_idx: usize,
    ) -> anyhow::Result<PathBuf> {
        let file = [
            self.sim_dir(mix, sim)?.as_path(),
            format!("records_{}.csv", path_idx).as_ref(),
        ]
        .into_iter()
        .collect();
        Ok(file)
    }

    fn elapsed_file(&self, mix: &Mix, sim: SimKind) -> anyhow::Result<PathBuf> {
        let file = [self.sim_dir(mix, sim)?.as_path(), "elapsed.txt".as_ref()]
            .into_iter()
            .collect();
        Ok(file)
    }

    fn elapsed_file_with_idx(&self, mix: &Mix, sim: SimKind, path_idx: usize) -> anyhow::Result<PathBuf> {
        let file = [self.sim_dir(mix, sim)?.as_path(), format!("elapsed_{}.txt", path_idx).as_ref()]
            .into_iter()
            .collect();
        Ok(file)
    }

    fn clustering_file(&self, mix: &Mix, sim: SimKind) -> anyhow::Result<PathBuf> {
        let file = [self.sim_dir(mix, sim)?.as_path(), "clustering.txt".as_ref()]
            .into_iter()
            .collect();
        Ok(file)
    }

    fn load_file(&self, mix: &Mix, sim: SimKind) -> anyhow::Result<PathBuf> {
        let file = [self.sim_dir(mix, sim)?.as_path(), "loads.json".as_ref()]
            .into_iter()
            .collect();
        Ok(file)
    }

    fn get_input_from_file(&self,file_path: PathBuf) -> io::Result<(FxHashMap<(NodeId, NodeId), HashSet<FlowId>>, FxHashMap<usize, HashSet<(NodeId, NodeId)>>)> {
        let mut channel_to_flowid_map: FxHashMap<(NodeId, NodeId), HashSet<FlowId>> = FxHashMap::default();
        let mut flowid_to_path_map: FxHashMap<usize, HashSet<(NodeId, NodeId)>> = FxHashMap::default();

        let file = fs::File::open(file_path)?;
        let reader = io::BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            let mut tmp = line.split(",");
            let src = tmp.nth(1).and_then(|x| x.parse::<NodeId>().ok());
            let dst = tmp.next().and_then(|x| x.parse::<NodeId>().ok());
            if let (Some(src), Some(dst)) = (src, dst) {
                let tmp_key = (src, dst);
                for val in tmp.skip(1).filter_map(|x| x.parse::<usize>().ok()) {
                    flowid_to_path_map
                        .entry(val)
                        .or_insert_with(HashSet::new)
                        .insert(tmp_key);
                    channel_to_flowid_map
                        .entry(tmp_key)
                        .or_insert_with(HashSet::new)
                        .insert(FlowId::new(val));
                }
            }
        }
        Ok((channel_to_flowid_map, flowid_to_path_map))
    }

    fn get_routes(
        &self,
        flowid_to_path_map: FxHashMap<usize, HashSet<(NodeId, NodeId)>>,
        flows: &Vec<Flow>, // Assuming flows is a vector of Flow objects
    ) -> (FxHashMap<Vec<(NodeId, NodeId)>, HashSet<usize>>, FxHashMap<usize, Vec<(NodeId, NodeId)>>) {
        let mut path_to_flowid_map: FxHashMap<Vec<(NodeId, NodeId)>, HashSet<usize>> = FxHashMap::default();
        let mut flowid_to_path_map_ordered: FxHashMap<usize, Vec<(NodeId, NodeId)>> = FxHashMap::default();

        for (flow_id, path) in flowid_to_path_map {
            let mut pairs = path.into_iter().collect::<Vec<_>>();
            pairs.sort();
            let mut path_ordered = Vec::<(NodeId, NodeId)>::with_capacity(pairs.len() + 1);
            path_ordered.push((flows[flow_id].src, flows[flow_id].dst));

            if let Some(first_pair) = pairs.first() {
                path_ordered.push(*first_pair);

                // Iterate over the remaining pairs
                while path_ordered.len() != pairs.len() + 1 {
                    for pair in pairs.iter().skip(1) {
                        // If the source of the current pair equals the destination of the last pair in the ordered list
                        if pair.0 == path_ordered.last().unwrap().1 {
                            path_ordered.push(*pair);
                        }
                    }
                }
            }
            path_to_flowid_map
                .entry(path_ordered.clone())
                .or_insert_with(HashSet::new)
                .insert(flow_id);
            flowid_to_path_map_ordered.insert(flow_id, path_ordered.clone());
        }

        (path_to_flowid_map, flowid_to_path_map_ordered)
    }

}

fn is_close_enough(a: &Option<DistsAndLoad>, b: &Option<DistsAndLoad>) -> bool {
    match (a, b) {
        (None, None) => true,
        (None, Some(_)) => false,
        (Some(_), None) => false,
        (Some(feat1), Some(feat2)) => {
            let sz_wmape = clustering::utils::wmape(&feat1.sizes, &feat2.sizes);
            let arr_wmape = clustering::utils::wmape(&feat1.deltas, &feat2.deltas);
            let max_wmape = std::cmp::max_by(sz_wmape, arr_wmape, |x, y| {
                x.partial_cmp(y)
                    .expect("`max_wmape_xs`: failed to compare floats")
            });
            (max_wmape < 0.1) && ((feat1.load - feat2.load).abs() < 0.005)
        }
    }
}

#[derive(Debug, Clone, Copy, clap::Subcommand, serde::Serialize, serde::Deserialize)]
pub enum SimKind {
    Ns3,
    Ns3Param,
    Ns3Config,
    Ns3PathOne,
    Ns3PathAll,
    Pmn,
    PmnM,
    PmnMParam,
    PmnMC,
    PmnMPath,
    Mlsys,
    MlsysParam,
    MlsysTest
}

impl fmt::Display for SimKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            SimKind::Ns3 => "ns3",
            SimKind::Ns3Config => "ns3-config",
            SimKind::Ns3Param => "ns3-param",
            SimKind::Ns3PathOne => "ns3-path-one",
            SimKind::Ns3PathAll => "ns3-path-all",
            SimKind::Pmn => "pmn_s1",
            SimKind::PmnM => "pmn-m",
            SimKind::PmnMParam => "pmn-m-param",
            SimKind::PmnMC => "pmn-mc",
            SimKind::PmnMPath => "pmn-m-path",
            SimKind::Mlsys => "mlsys-new_e476_timely",
            SimKind::MlsysParam => "mlsys-param",
            SimKind::MlsysTest => "mlsys-test",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct Record {
    pub mix_id: MixId,
    pub flow_id: FlowId,
    pub size: Bytes,
    pub slowdown: f64,
    pub sim: SimKind,
}
