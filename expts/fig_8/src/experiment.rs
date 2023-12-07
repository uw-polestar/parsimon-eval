use std::{
    collections::HashMap,
    collections::HashSet,
    fmt, fs,
    path::{Path, PathBuf},
    time::Instant,
    io::{self, BufRead}
};

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

use crate::flowsim::Flowsim;
use crate::mix::{Mix, MixId};

const NS3_DIR: &str = "../../../parsimon/backends/High-Precision-Congestion-Control/simulation";
const BASE_RTT: Nanosecs = Nanosecs::new(14_400);
const WINDOW: Bytes = Bytes::new(18_000);
const DCTCP_GAIN: f64 = 0.0625;
const DCTCP_AI: Mbps = Mbps::new(615);
const NR_FLOWS: usize = 2_000_000;
// const NR_FLOWS: usize = 2_000;

const FLOWSIM_PATH: &str = "./src/main_flowsim_mmf.py";
const PYTHON_PATH: &str = "/data1/lichenni/software/anaconda3/envs/py39/bin";

#[derive(Debug, clap::Parser)]
pub struct Experiment {
    #[clap(long, default_value = "./data")]
    root: PathBuf,
    #[clap(long)]
    mixes: PathBuf,
    #[clap(long, default_value_t = 0)]
    seed: u64,
    #[clap(subcommand)]
    sim: SimKind,
}

impl Experiment {
    pub fn run(&self) -> anyhow::Result<()> {
        let mixes: Vec<Mix> = serde_json::from_str(&fs::read_to_string(&self.mixes)?)?;
        // All ns3 simulations can run in parallel. Parsimon simulations are already massively
        // parallel, so they'll run one at a time to save memory.
        match self.sim {
            SimKind::Ns3 => {
                mixes.par_iter().try_for_each(|mix| self.run_ns3(mix))?;
            }
            SimKind::Pmn => {
                for mix in &mixes {
                    self.run_pmn(mix)?;
                }
            }
            SimKind::PmnM => {
                for mix in &mixes {
                    self.run_pmn_m(mix)?;
                }
            }
            SimKind::PmnMC => {
                for mix in &mixes {
                    self.run_pmn_mc(mix)?;
                }
            }
            SimKind::Ns3Path => {
                mixes
                    .par_iter()
                    .try_for_each(|mix| self.run_ns3_path(mix))?;
            }
            SimKind::Ns3PathAll => {
                mixes
                    .par_iter()
                    .try_for_each(|mix| self.run_ns3_path_all(mix))?;
            }
            SimKind::PmnMPath => {
                for mix in &mixes {
                    self.run_pmn_m_path(mix)?;
                }
            }
            SimKind::Flowsim => {
                for mix in &mixes {
                    self.run_flowsim(mix)?;
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

    fn run_ns3_path(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::Ns3Path;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;

        // read flows associated with a path
        let mut max_row=0;
        let mut max_col=0;
        let mut num_flows_in_f_prime=0;
        let mut flow_ids: Vec<FlowId> = Vec::new();

        let flow_on_path_file=self.flow_on_path_file(mix,sim)?;
        let file = fs::File::open(flow_on_path_file)?;

        // Create a buffered reader to efficiently read lines
        let reader = io::BufReader::new(file);
        for (line_number, line) in reader.lines().enumerate() {
            let line = line?;
            if line_number==0 {
                let tmp=line.split(",").collect::<Vec<_>>();
                max_row=tmp[0].parse::<usize>().unwrap();
                max_col=tmp[1].parse::<usize>().unwrap();
                num_flows_in_f_prime=tmp[2].parse::<usize>().unwrap();
            }
            else {
                flow_ids.push(line.trim().parse::<FlowId>().unwrap());
            }
        }

        let path_str = format!("{},{},{}", max_row, max_col,num_flows_in_f_prime);
        self.put_path(mix, sim, path_str)?;
        // println!("The selected path is ({:?}, {:?})", max_row,max_col);
        // get flows for a specific path
        let flows_remaining = flows
            .into_iter()
            .filter(|flow| flow_ids.contains(&flow.id))
            .collect::<Vec<_>>();

        let start = Instant::now(); // timer start
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

        // read flows associated with a path
        let flow_ids_in_f: HashSet<FlowId>;
        let mut flow_ids_in_f_prime: HashSet<FlowId> = HashSet::new();
        
        let mut channel_to_flowids_map: HashMap<(NodeId,NodeId),HashSet<FlowId>>=HashMap::new();
        let mut flow_to_path_map: HashMap<usize, HashSet<(NodeId,NodeId)>> = HashMap::new();
        let mut path_to_flows_map: HashMap<Vec<(NodeId,NodeId)>,HashSet<usize>> = HashMap::new();
        
        let flow_path_map_file=self.flow_path_map_file(mix,sim)?;
        let file = fs::File::open(flow_path_map_file)?;

        // Create a buffered reader to efficiently read lines
        let reader = io::BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
                let tmp=line.split(",").map(|x| x.parse::<usize>().unwrap()).collect::<Vec<_>>();
                let tmp_key=(NodeId::new(tmp[1]),NodeId::new(tmp[2]));
                for &val in &tmp[4..] {
                    flow_to_path_map.entry(val).or_insert_with(HashSet::new).insert(tmp_key);
                    channel_to_flowids_map.entry(tmp_key).or_insert_with(HashSet::new).insert(FlowId::new(val));
                }
        }
        for (flow_id, path) in flow_to_path_map {
            let mut key_vec=path.into_iter().collect::<Vec<_>>();
            key_vec.sort();
            key_vec.insert(0, (flows[flow_id].src,flows[flow_id].dst));
            path_to_flows_map.entry(key_vec).or_insert_with(HashSet::new).insert(flow_id);
        }
        
        let path=path_to_flows_map.iter().max_by_key(|x| x.1.len()).unwrap().0;
        flow_ids_in_f=path_to_flows_map[path].iter().map(|x| FlowId::new(*x)).collect::<HashSet<_>>();

        for pair in path {
            if channel_to_flowids_map.contains_key(&pair) {
                flow_ids_in_f_prime.extend(channel_to_flowids_map[&pair].iter());
            }
        }
       
        let path_str = path.iter().map(|&x| format!("{}-{}",x.0,x.1)).collect::<Vec<String>>().join(",");
        let flow_str=flow_ids_in_f.iter().map(|&x| x.to_string()).collect::<Vec<String>>().join("\n");
        self.put_path(mix, sim, format!("{},{}\n{}",path_str,flow_ids_in_f.len(),flow_str))?;
        // println!("The selected path is ({:?}, {:?})", max_row,max_col);

        // get flows for a specific path
        let flows_remaining = flows
            .into_iter()
            .filter(|flow| flow_ids_in_f_prime.contains(&flow.id))
            .collect::<Vec<_>>();

        let start = Instant::now(); // timer start
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
        let loads = network.link_loads().collect::<Vec<_>>();
        let linksim = Ns3Link::builder()
            .root_dir(self.sim_dir(mix, sim)?)
            .ns3_dir(NS3_DIR)
            .window(WINDOW)
            .base_rtt(BASE_RTT)
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
        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_loads(mix, sim, &loads)?;
        self.put_elapsed(mix, sim, elapsed_secs)?;
        self.put_records(mix, sim, &records)?;
        Ok(())
    }

    fn run_pmn_m(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::PmnM;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let start = Instant::now(); // timer start
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
        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_loads(mix, sim, &loads)?;
        self.put_elapsed(mix, sim, elapsed_secs)?;
        self.put_records(mix, sim, &records)?;
        Ok(())
    }

    fn run_pmn_m_path(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::PmnMPath;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;
        // construct SimNetwork
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let network = Network::new(&nodes, &links)?;
        let network = network.into_simulations(flows.clone());
        // get a specific path
        let mut flow_to_num_map: HashMap<(NodeId, NodeId), i32> = HashMap::new();
        for flow in flows.iter() {
            let key_tuple = (flow.src, flow.dst);
            match flow_to_num_map.get(&key_tuple) {
                Some(count) => {
                    flow_to_num_map.insert(key_tuple, count + 1);
                }
                None => {
                    flow_to_num_map.insert(key_tuple, 1);
                }
            }
        }
        let src_dst_pair = flow_to_num_map
            .iter()
            .max_by(|a, b| a.1.cmp(&b.1))
            .map(|(k, _v)| k)
            .unwrap();

        let max_row = src_dst_pair.0;
        let max_col = src_dst_pair.1;
        let path_str = format!("{},{}", max_row, max_col);
        self.put_path(mix, sim, path_str)?;
        // println!("The selected path is ({:?}, {:?})", max_row,max_col);
        // get flows for a specific path
        let path = network.path(max_row, max_col, |choices| choices.first());
        let flow_ids = path
            .iter()
            .flat_map(|(_, c)| c.flow_ids())
            .collect::<HashSet<_>>();
        let flows_remaining = flows
            .into_iter()
            .filter(|flow| flow_ids.contains(&flow.id))
            .collect::<Vec<_>>();

        let start = Instant::now(); // timer start
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

    fn run_flowsim(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::Flowsim;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;
        // construct SimNetwork
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let network = Network::new(&nodes, &links)?;
        let network = network.into_simulations(flows.clone());
        // get a specific path
        let mut flow_to_num_map: HashMap<(NodeId, NodeId), i32> = HashMap::new();
        for flow in flows.iter() {
            let key_tuple = (flow.src, flow.dst);
            match flow_to_num_map.get(&key_tuple) {
                Some(count) => {
                    flow_to_num_map.insert(key_tuple, count + 1);
                }
                None => {
                    flow_to_num_map.insert(key_tuple, 1);
                }
            }
        }
        let src_dst_pair = flow_to_num_map
            .iter()
            .max_by(|a, b| a.1.cmp(&b.1))
            .map(|(k, _v)| k)
            .unwrap();

        let max_row = src_dst_pair.0;
        let max_col = src_dst_pair.1;
        let path_str = format!("{},{}", max_row, max_col);
        self.put_path(mix, sim, path_str)?;
        // println!(
        //     "The selected path is ({:?}, {:?}), with flows of {:?}",
        //     max_row, max_col, flow_to_num_map[src_dst_pair]
        // );
        // get flows for a specific path
        let path = network.path(max_row, max_col, |choices| choices.first());
        let flow_ids = path
            .iter()
            .flat_map(|(_, c)| c.flow_ids())
            .collect::<HashSet<_>>();
        let mut flows_remaining = flows
            .into_iter()
            .filter(|flow| flow_ids.contains(&flow.id))
            .collect::<Vec<_>>();

        let mut flow_to_path_map: HashMap<FlowId, (usize, usize)> = HashMap::new();
        let mut path_length = 1;
        for (_, c) in path.iter() {
            let flows = c.flow_ids();
            for key_flowid in flows {
                // println!("flow {} is on path {}", key_flowid, idx);
                match flow_to_path_map.get(&key_flowid) {
                    Some(count) => {
                        // println!("flow {}: {} {} {}", key_flowid, count.0, count.1, idx);
                        // println!("flow {}: {} {} {}", key_flowid, count.0, count.1, idx);
                        // assert!(count.1 == idx);
                        flow_to_path_map.insert(key_flowid, (count.0, path_length));
                    }
                    None => {
                        flow_to_path_map.insert(key_flowid, (path_length - 1, path_length));
                    }
                }
            }
            path_length += 1;
        }
        for idx in 0..flows_remaining.len() {
            let flow = flows_remaining[idx];
            let src = NodeId::new(flow_to_path_map[&flow.id].0);
            let dst = NodeId::new(flow_to_path_map[&flow.id].1);
            flows_remaining[idx].src = src;
            flows_remaining[idx].dst = dst;
        }
        let start = Instant::now(); // timer start
        let flowsim = Flowsim::builder()
            .python_path(PYTHON_PATH)
            .script_path(FLOWSIM_PATH)
            .data_dir(self.sim_dir(mix, sim)?)
            .nodes(cluster.nodes().cloned().collect::<Vec<_>>())
            .links(cluster.links().cloned().collect::<Vec<_>>())
            .flows(flows_remaining)
            .build();
        let records = flowsim
            .run(path_length)?
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
        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_loads(mix, sim, &loads)?;
        self.put_clustering(mix, sim, frac)?;
        self.put_elapsed(mix, sim, elapsed_secs)?;
        self.put_records(mix, sim, &records)?;
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

    fn put_elapsed(&self, mix: &Mix, sim: SimKind, secs: u64) -> anyhow::Result<()> {
        fs::write(self.elapsed_file(mix, sim)?, secs.to_string())?;
        Ok(())
    }

    fn put_path(&self, mix: &Mix, sim: SimKind, path_str: String) -> anyhow::Result<()> {
        fs::write(self.path_file(mix, sim)?, path_str)?;
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

    fn flow_file(&self, mix: &Mix) -> anyhow::Result<PathBuf> {
        let file = [self.mix_dir(mix)?.as_path(), "flows.json".as_ref()]
            .into_iter()
            .collect();
        Ok(file)
    }

    fn flow_on_path_file(&self, mix: &Mix,sim: SimKind) -> anyhow::Result<PathBuf> {
        let file = [self.sim_dir(mix, sim)?.as_path(), "../ns3/flows_on_path.txt".as_ref()]
            .into_iter()
            .collect();
        Ok(file)
    }

    fn flow_path_map_file(&self, mix: &Mix,sim: SimKind) -> anyhow::Result<PathBuf> {
        let file = [self.sim_dir(mix, sim)?.as_path(), "../ns3/flows_path_map.txt".as_ref()]
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

    fn elapsed_file(&self, mix: &Mix, sim: SimKind) -> anyhow::Result<PathBuf> {
        let file = [self.sim_dir(mix, sim)?.as_path(), "elapsed.txt".as_ref()]
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
    Ns3Path,
    Ns3PathAll,
    PmnMPath,
    Flowsim,
}

impl fmt::Display for SimKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            SimKind::Ns3 => "ns3",
            SimKind::Pmn => "pmn",
            SimKind::PmnM => "pmn-m",
            SimKind::PmnMC => "pmn-mc",
            SimKind::Ns3Path => "ns3-path",
            SimKind::Ns3PathAll => "ns3-path-all",
            SimKind::PmnMPath => "pmn-m-path",
            SimKind::Flowsim => "flowsim",
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
