use std::{
    fmt, fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    time::Instant,
};

use log::info;
use parsimon::{
    core::{
        network::{Flow, FlowId, Network},
        opts::SimOpts,
        units::{Bytes, Mbps},
    },
    impls::{
        self,
        clustering::{
            feature::{self, DistsAndLoad},
            greedy::GreedyClustering,
        },
        linksim::MinimLink,
    },
};
use rand::prelude::*;
use rayon::prelude::*;
use workload::{
    fabric::{Cluster, FabricRoutes},
    flowgen::{FlowGenerator, StopWhen},
    spatial::SpatialData,
};

use crate::mix::{Mix, MixId};

// const NS3_DIR: &str = "../../../parsimon/backends/High-Precision-Congestion-Control/simulation";
// const BASE_RTT: Nanosecs = Nanosecs::new(14_400);
const WINDOW: Bytes = Bytes::new(18_000);
const DCTCP_GAIN: f64 = 0.0625;
const DCTCP_AI: Mbps = Mbps::new(615);

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
            SimKind::PmnM => self.run_pmn_m(&mix),
            SimKind::PmnMC => self.run_pmn_mc(&mix),
        }
    }

    fn run_pmn_m(&self, mix: &Mix) -> anyhow::Result<()> {
        let sim = SimKind::PmnM;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let flows = self.flows(mix)?;
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let start = Instant::now(); // timer start
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
        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_elapsed(mix, sim, elapsed_secs)?;
        self.put_records(mix, sim, &records)?;
        Ok(())
    }

    fn run_pmn_mc(&self, mix: &Mix) -> anyhow::Result<()> {
        info!("Running PMN-MC Simulation");
        let sim = SimKind::PmnMC;
        let cluster: Cluster = serde_json::from_str(&fs::read_to_string(&mix.cluster)?)?;
        let a = Instant::now();
        let flows = self.flows(mix)?;
        let b = a.elapsed().as_secs();
        info!("Getting flows took {b} seconds");
        let nodes = cluster.nodes().cloned().collect::<Vec<_>>();
        let links = cluster.links().cloned().collect::<Vec<_>>();
        let start = Instant::now(); // timer start
        let a = Instant::now();
        let network = Network::new_with_routes(&nodes, &links, FabricRoutes::new(&cluster))?;
        let b = a.elapsed().as_secs();
        info!("Building new network took {b} seconds");
        let a = Instant::now();
        let mut network = network.into_simulations(flows.clone());
        let b = a.elapsed().as_secs();
        info!("Into simulations took {b} seconds");
        let a = Instant::now();
        let clusterer = GreedyClustering::new(feature::dists_and_load, is_close_enough);
        network.cluster(clusterer);
        let b = a.elapsed().as_secs();
        info!("Clustering took {b} seconds");
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
        let a = Instant::now();
        let network = network.into_delays(sim_opts)?;
        let b = a.elapsed().as_secs();
        info!("Into delay network took {b} seconds");
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
        info!("Sampling took {b} seconds");
        let elapsed_secs = start.elapsed().as_secs(); // timer end
        self.put_clustering(mix, sim, frac)?;
        self.put_elapsed(mix, sim, elapsed_secs)?;
        self.put_records(mix, sim, &records)?;
        Ok(())
    }

    fn flows(&self, mix: &Mix) -> anyhow::Result<Vec<Flow>> {
        env_logger::init();
        let path = self.flow_file(mix)?;
        if !path.exists() {
            info!("Generating flows...");
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
            .stop_when(StopWhen::Elapsed(mix.duration))
            .seed(self.seed)
            .build();
        let flows = flowgen.generate();
        let s = rmp_serde::encode::to_vec(&flows)?;
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

    fn flow_file(&self, mix: &Mix) -> anyhow::Result<PathBuf> {
        let file = [self.mix_dir(mix)?.as_path(), "flows.msgpack".as_ref()]
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
            let sz_wmape = impls::clustering::utils::wmape(&feat1.sizes, &feat2.sizes);
            let arr_wmape = impls::clustering::utils::wmape(&feat1.deltas, &feat2.deltas);
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
    PmnM,
    PmnMC,
}

impl fmt::Display for SimKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            SimKind::PmnM => "pmn-m",
            SimKind::PmnMC => "pmn-mc",
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
