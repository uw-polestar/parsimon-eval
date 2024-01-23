use std::{
    fmt, fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    io::{self, BufRead},
    path::{Path, PathBuf},
    time::Instant, collections::HashSet, collections::HashMap, process::exit,
};

use anyhow::Ok;
use ns3_frontend::Ns3Simulation;
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
use parsimon::impls::linksim::{minim::MinimLink, ns3::Ns3Link};
use rand::prelude::*;
use rayon::prelude::*;
use workload::{
    fabric::Cluster,
    flowgen::{FlowGenerator, StopWhen},
    spatial::SpatialData,
};

use crate::mix::{Mix, MixId};

use rand::distributions::WeightedIndex;
use rustc_hash::FxHashMap;
use crate::mlsys::{
    Mlsys,
    ns3_clean
};

const NS3_DIR: &str = "../../../parsimon/backends/High-Precision-Congestion-Control/simulation";
const BASE_RTT: Nanosecs = Nanosecs::new(14_400);
const WINDOW: Bytes = Bytes::new(18_000);
const DCTCP_GAIN: f64 = 0.0625;
const DCTCP_AI: Mbps = Mbps::new(615);
const NR_FLOWS: usize = 31_647_250; //11_351_649, 31_647_250;
const NR_PATHS_SAMPLED: usize = 500;
const NR_SIZE_BUCKETS: usize = 4;
const OUTPUT_LEN: usize = 100;
const FLOWS_ON_PATH_THRESHOLD: usize = 1;
const SAMPLE_MODE: usize = 1;

const MLSYS_PATH: &str = "../../../fast-mmf-fattree";

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
            SimKind::PmnPath => self.run_pmn_path(&mix),
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
            .window(WINDOW)
            .base_rtt(BASE_RTT)
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
        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_elapsed(mix, sim, elapsed_secs)?;
        self.put_records(mix, sim, &records)?;
        Ok(())
    }

    fn run_mlsys(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::Mlsys;
        let flows = self.flows(mix)?;
        // read flows associated with a path
        let mut channel_to_flowid_map: FxHashMap<(NodeId, NodeId), HashSet<FlowId>> = FxHashMap::default();
        let mut flowid_to_path_map: FxHashMap<usize, HashSet<(NodeId, NodeId)>> = FxHashMap::default();
        let mut flowid_to_path_map_ordered: FxHashMap<usize, Vec<(NodeId, NodeId)>> = FxHashMap::default();
        let mut path_to_flowid_map: FxHashMap<Vec<(NodeId, NodeId)>, HashSet<usize>> = FxHashMap::default();
        let flowid_to_flow_map: FxHashMap<FlowId, Flow> = flows
            .iter()
            .map(|flow| (flow.id, flow.clone()))
            .collect::<FxHashMap<_, _>>();
        let flow_path_map_file = self.flow_path_map_file(mix, sim)?;
        let file = fs::File::open(flow_path_map_file)?;

        let start_1 = Instant::now(); // timer start
        
        // Create a buffered reader to efficiently read lines
        let reader = io::BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            let mut tmp = line
                .split(",");
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

        let start_extra = Instant::now(); // timer start

        for (flow_id, path) in flowid_to_path_map {
            let mut pairs = path.into_iter().collect::<Vec<_>>();
            pairs.sort();
            let mut path_ordered = Vec::<(NodeId, NodeId)>::with_capacity(pairs.len() + 1);
            path_ordered.push((flows[flow_id].src, flows[flow_id].dst));

            if let Some(first_pair) = pairs.first() {
                path_ordered.push(*first_pair);

                // Iterate over the remaining pairs
                while path_ordered.len() != pairs.len()+1 {
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

        let elapsed_secs_extra = start_extra.elapsed().as_secs(); // timer end

        let path_to_flows_vec_sorted = path_to_flowid_map
            .iter()
            .filter(|(_, value)| value.len() >= FLOWS_ON_PATH_THRESHOLD)
            .collect::<Vec<_>>();
        // let mut length_counts: HashMap<usize, usize> = HashMap::new();
        // for (path, _) in path_to_flows_vec_sorted.iter() {
        //     *length_counts.entry(path.len()).or_insert(0) += 1;
        // }
        // for (length, count) in length_counts {
        //     println!("Key length: {}, Occurrences: {}. Time: {}", length, count, elapsed_secs_extra);
        // }

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

        let mut path_to_flows_map_str = String::new();
        for (key, value) in &path_to_flowid_map {
            path_to_flows_map_str.push_str(&format!("{}:{}\n", key.iter()
            .map(|&x| format!("{}-{}", x.0, x.1))
            .collect::<Vec<String>>()
            .join("|"), value.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",")));
        }
        
        self.put_path2flows(mix, sim, path_to_flows_map_str)?;

        Ok(())
    }

    fn run_pmn_path(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::PmnPath;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;
        let start = Instant::now(); // timer start
        // construct SimNetwork
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let network = Network::new(&nodes, &links)?;
        let network = network.into_simulations(flows.clone());
        // get a specific path
        let mut flow_to_num_map: HashMap<(NodeId,NodeId), i32> = HashMap::new();
        for flow in flows.iter(){
            let key_tuple=(flow.src,flow.dst);
            match flow_to_num_map.get(&key_tuple) {
                Some(count) => { flow_to_num_map.insert(key_tuple, count + 1); }
                None => { flow_to_num_map.insert(key_tuple, 1); }
            }
        }
        let src_dst_pair = flow_to_num_map.iter().max_by(|a, b| a.1.cmp(&b.1)).map(|(k, _v)| k).unwrap();

        let max_row=src_dst_pair.0;
        let max_col=src_dst_pair.1;
        let path_str=format!("{},{}", max_row,max_col);
        self.put_path(mix, sim, path_str)?;
        // println!("The selected path is ({:?}, {:?})", max_row,max_col);
        // get flows for a specific path
        let path= network.path(max_row, max_col, |choices| choices.first());
        let flow_ids=path.iter().flat_map(|(_,c)| c.flow_ids()).collect::<HashSet<_>>();
        let flows_remaining=flows.into_iter().filter(|flow| flow_ids.contains(&flow.id)).collect::<Vec<_>>();
        let ns3 = Ns3Simulation::builder()
            .ns3_dir(NS3_DIR)
            .data_dir(self.sim_dir(mix, SimKind::PmnPath)?)
            .nodes(cluster.nodes().cloned().collect::<Vec<_>>())
            .links(cluster.links().cloned().collect::<Vec<_>>())
            .window(WINDOW)
            .base_rtt(BASE_RTT)
            .flows(flows_remaining)
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
        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_elapsed(mix, sim, elapsed_secs)?;
        self.put_records(mix, sim, &records)?;
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
            .window(WINDOW)
            .base_rtt(BASE_RTT)
            .build();
        let sim_opts = SimOpts::builder()
            .link_sim(linksim)
            .workers(self.workers.clone())
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
        // let loads = network.link_loads().collect::<Vec<_>>();
        // self.put_loads(mix, sim, &loads)?;
        let linksim = MinimLink::builder()
            .window(WINDOW)
            .dctcp_gain(DCTCP_GAIN)
            .dctcp_ai(DCTCP_AI)
            .build();
        let b = a.elapsed().as_secs();
        println!("Setup took {b} seconds");
        let sim_opts = SimOpts::builder()
            .link_sim(linksim)
            .workers(self.workers.clone())
            .build();
        let network = network.into_delays(sim_opts)?;
        let a = Instant::now();
        let records: Vec<_> = flows
            .par_iter()
            .enumerate()
            .filter_map(|(i, f)| {
                let seed = self.seed + i as u64;
                let mut rng = StdRng::seed_from_u64(seed + i as u64);
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
            .window(WINDOW)
            .dctcp_gain(DCTCP_GAIN)
            .dctcp_ai(DCTCP_AI)
            .build();
        let sim_opts = SimOpts::builder()
            .link_sim(linksim)
            .workers(self.workers.clone())
            .build();
        let network = network.into_delays(sim_opts)?;
        let records: Vec<_> = flows
            .par_iter()
            .enumerate()
            .filter_map(|(i, f)| {
                let seed = self.seed + i as u64;
                let mut rng = StdRng::seed_from_u64(seed + i as u64);
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
        self.put_clustering(mix, sim, frac)?;
        self.put_elapsed(mix, sim, elapsed_secs)?;
        self.put_records(mix, sim, &records)?;
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

    fn put_path2flows(&self, mix: &Mix, sim: SimKind, path_str: String) -> anyhow::Result<()> {
        if !self.path2flows_file(mix, sim)?.exists(){
            fs::write(self.path2flows_file(mix, sim)?, path_str)?;
        } 
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

    fn path2flows_file(&self, mix: &Mix, sim: SimKind) -> anyhow::Result<PathBuf> {
        let file = [self.sim_dir(mix, sim)?.as_path(), "path_to_flows.txt".as_ref()]
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
    PmnPath,
    Mlsys,
}

impl fmt::Display for SimKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            SimKind::Ns3 => "ns3",
            SimKind::Pmn => "pmn",
            SimKind::PmnM => "pmn-m",
            SimKind::PmnMC => "pmn-mc",
            SimKind::PmnPath => "pmn-path",
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
