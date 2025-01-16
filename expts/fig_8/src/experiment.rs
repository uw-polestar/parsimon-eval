use std::{
    fmt::{self}, fs,
    path::{Path, PathBuf},
    time::Instant,
};

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

use rustc_hash::{FxHashMap,FxHashSet};
use crate::ns3::Ns3Simulation;
use crate::ns3link::Ns3Link;

const NS3_DIR: &str = "../../../High-Precision-Congestion-Control/ns-3.39";

const BASE_RTT: Nanosecs = Nanosecs::new(14_400);
const DCTCP_GAIN: f64 = 0.0625;
const DCTCP_AI: Mbps = Mbps::new(615);
const NR_FLOWS: usize = 20_000;
// const NR_FLOWS: usize = 2_000;

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
                mixes.par_iter().try_for_each(|mix| self.run_ns3(mix, false))?; 
            }
           
            SimKind::Mlsys => {
                for mix in &mixes {
                    self.run_ns3(mix, true)?;
                }
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
            
        }
        Ok(())
    }

    fn run_ns3(&self, mix: &Mix, enable_mlsys: bool) -> anyhow::Result<()> {
        let sim = SimKind::Ns3;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;
        
        // let start_read = Instant::now(); // timer start
        // construct SimNetwork
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let network = Network::new(&nodes, &links)?;
        let network = network.into_simulations_path(flows.clone());
        let (_channel_to_flowid_map, path_to_flowid_map): (
            &FxHashMap<(NodeId, NodeId), FxHashSet<FlowId>>,
            &FxHashMap<Vec<(NodeId, NodeId)>, FxHashSet<FlowId>>
        ) = match network.get_routes() {
            Some((channel_map, path_map)) => (channel_map, path_map),
            None => panic!("Routes not available"),
        };
        

        // println!("Path to FlowID Map length: {}", path_to_flowid_map.len());
        // Step 1: Create a new HashMap to store FlowId -> Path mapping
        let mut flowid_to_path_map: FxHashMap<FlowId, Vec<(NodeId, NodeId)>> = FxHashMap::default();
        for (path, flow_ids) in path_to_flowid_map.iter() {
            for flow_id in flow_ids {
                // Insert the flow ID and corresponding path into the reverse map
                flowid_to_path_map.insert(*flow_id, path.clone());
            }
        }
        
        let mut sorted_flowid_to_path_map: Vec<(&FlowId, &Vec<(NodeId, NodeId)>)> = flowid_to_path_map.iter().collect();
        sorted_flowid_to_path_map.sort_by_key(|&(flow_id, _)| flow_id);
        
        // Step 3: Write the sorted FlowId -> Path mapping into a file
        let mut results_str_flowid = String::new();
        for (flow_id, path) in sorted_flowid_to_path_map.iter() {
            results_str_flowid.push_str(&format!("{}:", flow_id));

            // Append the nodes in the path
            for (node_a, node_b) in path.iter() {
                results_str_flowid.push_str(&format!("{}-{}", node_a, node_b));
                results_str_flowid.push_str(",");
            }
            results_str_flowid.push_str("\n");
        }

        self.put_path_with_idx(
            mix,
            sim,
            1,
            format!(
                "{},{}\n{}",
                flowid_to_path_map.len(),
                path_to_flowid_map.len(),
                results_str_flowid
            ),
        )
        .unwrap();
        // let elapsed_read= start_read.elapsed().as_secs();
        // println!("read time-{}: {}", mix.id,elapsed_read);

        let start = Instant::now(); // timer start
        let ns3 = Ns3Simulation::builder()
            .ns3_dir(NS3_DIR)
            .data_dir(self.sim_dir(mix, sim)?)
            .nodes(cluster.nodes().cloned().collect::<Vec<_>>())
            .links(cluster.links().cloned().collect::<Vec<_>>())
            .base_rtt(BASE_RTT)
            .flows(flows)
            .mix_id(mix.id)
            .bfsz(mix.bfsz)
            .window(Bytes::new(mix.window))
            .enable_pfc(mix.enable_pfc)
            .cc_kind(mix.cc)
            .param_1(mix.param_1)
            .param_2(mix.param_2)
            .max_inflight_flows(mix.max_inflight_flows)
            .enable_mlsys(enable_mlsys)
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
            // .window(WINDOW)
            .dctcp_gain(DCTCP_GAIN)
            .dctcp_ai(DCTCP_AI)
            .window(Bytes::new(mix.window))
            .dctcp_k(mix.param_1)
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
            // .window(WINDOW)
            .window(Bytes::new(mix.window))
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

    fn put_elapsed(&self, mix: &Mix, sim: SimKind, secs: u64) -> anyhow::Result<()> {
        fs::write(self.elapsed_file(mix, sim)?, secs.to_string())?;
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

    fn flow_file(&self, mix: &Mix) -> anyhow::Result<PathBuf> {
        let file = [self.mix_dir(mix)?.as_path(), "flows.json".as_ref()]
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
            SimKind::Ns3 => "ns3",
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
