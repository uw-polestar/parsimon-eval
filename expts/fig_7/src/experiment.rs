use std::{
    collections::HashSet,
    fmt, fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::Ok;
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

use crate::mix::{Mix, MixId};

use rand::distributions::WeightedIndex;
use rustc_hash::{FxHashMap,FxHashSet};
use crate::mlsys::Mlsys;
use crate::ns3::Ns3Simulation;
use crate::ns3link::Ns3Link;

const NS3_DIR: &str = "../../../parsimon/backends/High-Precision-Congestion-Control/simulation";
const BASE_RTT: Nanosecs = Nanosecs::new(14_400);
const DCTCP_GAIN: f64 = 0.0625;
const DCTCP_AI: Mbps = Mbps::new(615);
const NR_FLOWS: usize = 20_000_000; //11_351_649, 15_872_306, 31_647_250;
const NR_PATHS_SAMPLED: usize = 500;
const NR_SIZE_BUCKETS: usize = 4;
const OUTPUT_LEN: usize = 100;

const MLSYS_PATH: &str = "../../../clibs";
const MODEL_SUFFIX: &str = "_large";

#[derive(Debug, clap::Parser)]
pub struct Experiment {
    #[clap(long, default_value = "./data")]
    root: PathBuf,
    #[clap(long)]
    mix: PathBuf,
    #[clap(long, default_value_t = 0)]
    seed: u64,
    #[clap(short, long, default_values_t = vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)])]
    workers: Vec<SocketAddr>,
    #[clap(subcommand)]
    sim: SimKind,
}

impl Experiment {
    pub fn run(&self) -> anyhow::Result<()> {
        let mix: Mix = serde_json::from_str(&fs::read_to_string(&self.mix)?)?;
        match self.sim {
            SimKind::Ns3 => self.run_ns3(&mix),
            SimKind::Pmn => self.run_pmn(&mix),
            SimKind::PmnM => self.run_pmn_m(&mix),
            SimKind::PmnMC => self.run_pmn_mc(&mix),
            SimKind::Mlsys => self.run_mlsys(&mix),
        }
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

        let path_to_flows_vec_sorted=path_to_flowid_map.iter().collect::<Vec<_>>();
        // let mut path_to_flows_vec_sorted = path_to_flowid_map
        //     .iter()
        //     .filter(|(_, value)| value.len() >= 1)
        //     .collect::<Vec<_>>();
        // path_to_flows_vec_sorted.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then(b.0[0].cmp(&a.0[0])));
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

    fn run_pmn(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::Pmn;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let start = Instant::now(); // timer start
        let network = Network::new(&nodes, &links)?;
        let network = network.into_simulations(flows.clone());
        let linksim = Ns3Link::builder()
            .root_dir(self.sim_dir(mix, SimKind::Pmn)?)
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
        let sim_opts = SimOpts::builder()
            .link_sim(linksim)
            .build();
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
        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_elapsed(mix, sim, elapsed_secs)?;
        self.put_records(mix, sim, &records)?;
        Ok(())
    }

    fn run_pmn_m(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::PmnM;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;

        let start = Instant::now(); // timer start
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let a = Instant::now();
        let network = Network::new(&nodes, &links)?;
        let network = network.into_simulations(flows.clone());
        // println!("Collecting link loads! Don't use perf results!");
        let loads = network.link_loads().collect::<Vec<_>>();
        self.put_loads(mix, sim, &loads)?;
        let linksim = MinimLink::builder()
            // .window(WINDOW)
            .dctcp_gain(DCTCP_GAIN)
            .dctcp_ai(DCTCP_AI)
            .window(Bytes::new(mix.window))
            .dctcp_k(mix.param_1)
            .build();
        let b = a.elapsed().as_secs();
        println!("Setup took {b} seconds");
        let sim_opts = SimOpts::builder()
            .link_sim(linksim)
            .build();
        let network = network.into_delays(sim_opts)?;
        let a = Instant::now();
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
        let b = a.elapsed().as_secs();
        println!("Sampling took {b} seconds");
        self.put_records(mix, sim, &records)?;
        
        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_elapsed(mix, sim, elapsed_secs)?;
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
        let clusterer = GreedyClustering::new(feature::dists_and_load, is_close_enough);
        network.cluster(&clusterer);
        let nr_clusters = network.clusters().len();
        let frac = nr_clusters as f64 / (links.len() * 2) as f64;
        let linksim = MinimLink::builder()
            .window(Bytes::new(mix.window))
            .dctcp_gain(DCTCP_GAIN)
            .dctcp_ai(DCTCP_AI)
            .build();
        let sim_opts = SimOpts::builder()
            .link_sim(linksim)
            .build();
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
        
        self.put_clustering(mix, sim, frac)?;
        self.put_records(mix, sim, &records)?;
        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_elapsed(mix, sim, elapsed_secs)?;
        Ok(())
    }

    fn flows(&self, mix: &Mix) -> anyhow::Result<Vec<Flow>> {
        let path = self.flow_file(mix)?;
        if !path.exists() {
            println!("Generating flows...");
            self.gen_flows(mix, &path)?;
            println!("Done.");
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
            .stop_when(StopWhen::Elapsed(mix.duration))
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

    fn put_elapsed(&self, mix: &Mix, sim: SimKind, secs: u64) -> anyhow::Result<()> {
        fs::write(self.elapsed_file(mix, sim)?, secs.to_string())?;
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

    #[allow(unused)]
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
    Pmn,
    PmnM,
    PmnMC,
    Mlsys,
}

impl fmt::Display for SimKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            SimKind::Ns3 => "ns3-config",
            SimKind::Pmn => "pmn",
            SimKind::PmnM => "pmn-m",
            SimKind::PmnMC => "pmn-mc",
            SimKind::Mlsys => "mlsys",
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
